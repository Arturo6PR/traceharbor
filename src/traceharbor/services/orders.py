"""Orders service orchestrating payment and inventory calls."""

from __future__ import annotations

import os
from typing import Annotated

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse

from traceharbor.contracts import (
    CheckoutRequest,
    InventoryRequest,
    Outcome,
    OutcomeCounts,
    PaymentRequest,
    ScenarioReport,
    ServiceStep,
    StepStatus,
)
from traceharbor.gateways import (
    HttpInventoryGateway,
    HttpPaymentGateway,
    InventoryGateway,
    PaymentGateway,
)
from traceharbor.services.common import require_scenario, require_service_context
from traceharbor.telemetry import EventSink, JsonLineEventSink, NullEventSink, emit_step
from traceharbor.tracecontext import IdFactory, RandomIdFactory


def create_orders_app(
    payment_gateway: PaymentGateway,
    inventory_gateway: InventoryGateway,
    *,
    id_factory: IdFactory | None = None,
    event_sink: EventSink | None = None,
) -> FastAPI:
    ids = id_factory or RandomIdFactory()
    sink = event_sink or NullEventSink()
    app = FastAPI(title="TraceHarbor Orders", version="0.1.0")

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"service": "orders", "status": "ok"}

    @app.post("/v1/orders", response_model=ScenarioReport)
    async def create_order(
        request: CheckoutRequest,
        traceparent: Annotated[str | None, Header()] = None,
        scenario_header: Annotated[str | None, Header(alias="x-traceharbor-scenario")] = None,
    ) -> ScenarioReport | JSONResponse:
        scenario = require_scenario(scenario_header)
        context = require_service_context(traceparent, ids)
        downstream_steps: list[ServiceStep] = []

        payment = await payment_gateway.charge(
            PaymentRequest(
                order_id=request.order_id,
                amount_cents=request.amount_cents,
                currency=request.currency,
            ),
            context,
            scenario,
        )
        downstream_steps.append(payment)

        if payment.status is not StepStatus.FAILED:
            inventory = await inventory_gateway.reserve(
                InventoryRequest(
                    order_id=request.order_id,
                    item_id=request.item_id,
                    quantity=request.quantity,
                ),
                context,
                scenario,
            )
            downstream_steps.append(inventory)

        outcome = _outcome(downstream_steps)
        order_status = {
            Outcome.HEALTHY: StepStatus.OK,
            Outcome.DEGRADED: StepStatus.DEGRADED,
            Outcome.FAILED: StepStatus.FAILED,
        }[outcome]
        order_detail = {
            Outcome.HEALTHY: "order completed",
            Outcome.DEGRADED: "order completed with degraded dependency",
            Outcome.FAILED: "order failed because a dependency failed",
        }[outcome]
        order_step = ServiceStep(
            service="orders",
            status=order_status,
            detail=order_detail,
            trace_id=context.trace_id,
            span_id=context.span_id,
            parent_span_id=context.parent_span_id,
        )
        emit_step(sink, scenario, order_step)
        steps = (order_step, *downstream_steps)
        report = ScenarioReport(
            scenario=scenario,
            outcome=outcome,
            order_id=request.order_id,
            trace_id=context.trace_id,
            counts=_counts(steps),
            steps=steps,
        )
        if outcome is Outcome.FAILED:
            return JSONResponse(status_code=424, content=report.model_dump(mode="json"))
        return report

    return app


def _outcome(steps: list[ServiceStep]) -> Outcome:
    statuses = {step.status for step in steps}
    if StepStatus.FAILED in statuses:
        return Outcome.FAILED
    if StepStatus.DEGRADED in statuses:
        return Outcome.DEGRADED
    return Outcome.HEALTHY


def _counts(steps: tuple[ServiceStep, ...]) -> OutcomeCounts:
    return OutcomeCounts(
        ok=sum(step.status is StepStatus.OK for step in steps),
        degraded=sum(step.status is StepStatus.DEGRADED for step in steps),
        failed=sum(step.status is StepStatus.FAILED for step in steps),
    )


def create_live_app() -> FastAPI:
    payment_url = os.environ.get("TRACEHARBOR_PAYMENT_URL", "http://127.0.0.1:8002")
    inventory_url = os.environ.get("TRACEHARBOR_INVENTORY_URL", "http://127.0.0.1:8003")
    return create_orders_app(
        HttpPaymentGateway(payment_url),
        HttpInventoryGateway(inventory_url),
        event_sink=JsonLineEventSink(),
    )


app = create_live_app()

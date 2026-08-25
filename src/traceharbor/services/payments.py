"""Payment service and deterministic fault boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse

from traceharbor import __version__
from traceharbor.contracts import PaymentRequest, ServiceStep
from traceharbor.observability import TelemetryRuntime
from traceharbor.scenarios import PROFILES
from traceharbor.services.common import require_scenario, require_service_context
from traceharbor.telemetry import EventSink, JsonLineEventSink, NullEventSink, emit_step
from traceharbor.tracecontext import IdFactory, RandomIdFactory

SleepFunction = Callable[[float], Awaitable[None]]


def create_payment_app(
    *,
    id_factory: IdFactory | None = None,
    event_sink: EventSink | None = None,
    observability: TelemetryRuntime | None = None,
    sleep: SleepFunction = asyncio.sleep,
) -> FastAPI:
    ids = id_factory or RandomIdFactory()
    sink = event_sink or NullEventSink()
    telemetry = observability or TelemetryRuntime.disabled("payments")
    app = FastAPI(title="TraceHarbor Payments", version=__version__)

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"service": "payments", "status": "ok"}

    @app.post("/v1/charges", response_model=ServiceStep)
    async def charge(
        request: PaymentRequest,
        traceparent: Annotated[str | None, Header()] = None,
        scenario_header: Annotated[str | None, Header(alias="x-traceharbor-scenario")] = None,
    ) -> ServiceStep | JSONResponse:
        del request
        scenario = require_scenario(scenario_header)
        context = require_service_context(traceparent, ids)
        behavior = PROFILES[scenario].payment
        if behavior.simulated_delay_ms:
            await sleep(behavior.simulated_delay_ms / 1000)
        step = ServiceStep(
            service="payments",
            status=behavior.status,
            detail=behavior.detail,
            trace_id=context.trace_id,
            span_id=context.span_id,
            parent_span_id=context.parent_span_id,
            simulated_delay_ms=behavior.simulated_delay_ms,
        )
        emit_step(sink, scenario, step)
        telemetry.record_step(scenario, step)
        if behavior.status_code >= 400:
            return JSONResponse(
                status_code=behavior.status_code,
                content=step.model_dump(mode="json"),
            )
        return step

    telemetry.instrument_app(app)
    app.router.add_event_handler("shutdown", telemetry.shutdown)
    return app


def create_live_app() -> FastAPI:
    return create_payment_app(
        event_sink=JsonLineEventSink(),
        observability=TelemetryRuntime.from_environment("payments"),
    )

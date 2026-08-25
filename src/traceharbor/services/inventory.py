"""Inventory service and deterministic fault boundary."""

from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse

from traceharbor.contracts import InventoryRequest, ServiceStep
from traceharbor.scenarios import PROFILES
from traceharbor.services.common import require_scenario, require_service_context
from traceharbor.telemetry import EventSink, JsonLineEventSink, NullEventSink, emit_step
from traceharbor.tracecontext import IdFactory, RandomIdFactory


def create_inventory_app(
    *,
    id_factory: IdFactory | None = None,
    event_sink: EventSink | None = None,
) -> FastAPI:
    ids = id_factory or RandomIdFactory()
    sink = event_sink or NullEventSink()
    app = FastAPI(title="TraceHarbor Inventory", version="0.1.0")

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"service": "inventory", "status": "ok"}

    @app.post("/v1/reservations", response_model=ServiceStep)
    async def reserve(
        request: InventoryRequest,
        traceparent: Annotated[str | None, Header()] = None,
        scenario_header: Annotated[str | None, Header(alias="x-traceharbor-scenario")] = None,
    ) -> ServiceStep | JSONResponse:
        del request
        scenario = require_scenario(scenario_header)
        context = require_service_context(traceparent, ids)
        behavior = PROFILES[scenario].inventory
        step = ServiceStep(
            service="inventory",
            status=behavior.status,
            detail=behavior.detail,
            trace_id=context.trace_id,
            span_id=context.span_id,
            parent_span_id=context.parent_span_id,
            simulated_delay_ms=behavior.simulated_delay_ms,
        )
        emit_step(sink, scenario, step)
        if behavior.status_code >= 400:
            return JSONResponse(
                status_code=behavior.status_code,
                content=step.model_dump(mode="json"),
            )
        return step

    return app


app = create_inventory_app(event_sink=JsonLineEventSink())

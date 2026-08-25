"""Shared HTTP boundary helpers."""

from fastapi import HTTPException

from traceharbor.contracts import Scenario
from traceharbor.scenarios import parse_scenario
from traceharbor.tracecontext import IdFactory, TraceContext, service_context


def require_scenario(value: str | None) -> Scenario:
    try:
        return parse_scenario(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def require_service_context(value: str | None, id_factory: IdFactory) -> TraceContext:
    try:
        return service_context(value, id_factory)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

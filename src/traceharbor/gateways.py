"""Downstream service gateways used by the orders service."""

from __future__ import annotations

from typing import Protocol

import httpx

from traceharbor.contracts import (
    InventoryRequest,
    PaymentRequest,
    Scenario,
    ServiceStep,
    StepStatus,
)
from traceharbor.tracecontext import IdFactory, RandomIdFactory, TraceContext


class PaymentGateway(Protocol):
    async def charge(
        self, request: PaymentRequest, parent: TraceContext, scenario: Scenario
    ) -> ServiceStep: ...


class InventoryGateway(Protocol):
    async def reserve(
        self, request: InventoryRequest, parent: TraceContext, scenario: Scenario
    ) -> ServiceStep: ...


class HttpPaymentGateway:
    def __init__(self, base_url: str, id_factory: IdFactory | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._id_factory = id_factory or RandomIdFactory()

    async def charge(
        self, request: PaymentRequest, parent: TraceContext, scenario: Scenario
    ) -> ServiceStep:
        try:
            async with httpx.AsyncClient(base_url=self._base_url, timeout=3.0) as client:
                response = await client.post(
                    "/v1/charges",
                    json=request.model_dump(mode="json"),
                    headers={
                        "traceparent": parent.traceparent,
                        "x-traceharbor-scenario": scenario.value,
                    },
                )
            return _validate_lineage(
                ServiceStep.model_validate(response.json()), "payments", parent
            )
        except (httpx.HTTPError, ValueError) as exc:
            context = self._id_factory.child(parent)
            return ServiceStep(
                service="payments",
                status=StepStatus.FAILED,
                detail=f"payment transport failure: {type(exc).__name__}",
                trace_id=context.trace_id,
                span_id=context.span_id,
                parent_span_id=context.parent_span_id,
            )


class HttpInventoryGateway:
    def __init__(self, base_url: str, id_factory: IdFactory | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._id_factory = id_factory or RandomIdFactory()

    async def reserve(
        self, request: InventoryRequest, parent: TraceContext, scenario: Scenario
    ) -> ServiceStep:
        try:
            async with httpx.AsyncClient(base_url=self._base_url, timeout=3.0) as client:
                response = await client.post(
                    "/v1/reservations",
                    json=request.model_dump(mode="json"),
                    headers={
                        "traceparent": parent.traceparent,
                        "x-traceharbor-scenario": scenario.value,
                    },
                )
            return _validate_lineage(
                ServiceStep.model_validate(response.json()), "inventory", parent
            )
        except (httpx.HTTPError, ValueError) as exc:
            context = self._id_factory.child(parent)
            return ServiceStep(
                service="inventory",
                status=StepStatus.FAILED,
                detail=f"inventory transport failure: {type(exc).__name__}",
                trace_id=context.trace_id,
                span_id=context.span_id,
                parent_span_id=context.parent_span_id,
            )


class ClientPaymentGateway:
    """Gateway over an injected client, useful for in-process demos and tests."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def charge(
        self, request: PaymentRequest, parent: TraceContext, scenario: Scenario
    ) -> ServiceStep:
        response = await self._client.post(
            "/v1/charges",
            json=request.model_dump(mode="json"),
            headers={
                "traceparent": parent.traceparent,
                "x-traceharbor-scenario": scenario.value,
            },
        )
        return _validate_lineage(ServiceStep.model_validate(response.json()), "payments", parent)


class ClientInventoryGateway:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def reserve(
        self, request: InventoryRequest, parent: TraceContext, scenario: Scenario
    ) -> ServiceStep:
        response = await self._client.post(
            "/v1/reservations",
            json=request.model_dump(mode="json"),
            headers={
                "traceparent": parent.traceparent,
                "x-traceharbor-scenario": scenario.value,
            },
        )
        return _validate_lineage(ServiceStep.model_validate(response.json()), "inventory", parent)


def _validate_lineage(
    step: ServiceStep, expected_service: str, parent: TraceContext
) -> ServiceStep:
    if step.service != expected_service:
        raise ValueError(
            f"downstream returned service {step.service!r}, expected {expected_service!r}"
        )
    if step.trace_id != parent.trace_id:
        raise ValueError("downstream returned a different trace ID")
    if step.parent_span_id != parent.span_id:
        raise ValueError("downstream returned an unexpected parent span ID")
    return step

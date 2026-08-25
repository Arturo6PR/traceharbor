import asyncio

import httpx
import pytest
from fastapi import FastAPI

from traceharbor.contracts import InventoryRequest, PaymentRequest, Scenario, StepStatus
from traceharbor.gateways import ClientPaymentGateway
from traceharbor.services.inventory import create_inventory_app
from traceharbor.services.payments import create_payment_app
from traceharbor.telemetry import CollectingEventSink
from traceharbor.tracecontext import DeterministicIdFactory, TraceContext


async def _no_sleep(seconds: float) -> None:
    del seconds


async def _request(app, method: str, path: str, **kwargs) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://service.test"
    ) as client:
        return await client.request(method, path, **kwargs)


def test_health_endpoints() -> None:
    payment = create_payment_app()
    inventory = create_inventory_app()

    assert asyncio.run(_request(payment, "GET", "/healthz")).json() == {
        "service": "payments",
        "status": "ok",
    }
    assert asyncio.run(_request(inventory, "GET", "/healthz")).json() == {
        "service": "inventory",
        "status": "ok",
    }


def test_payment_failure_preserves_trace_and_emits_structured_event() -> None:
    ids = DeterministicIdFactory("service-test")
    parent = ids.root()
    sink = CollectingEventSink()
    app = create_payment_app(id_factory=ids, event_sink=sink, sleep=_no_sleep)
    response = asyncio.run(
        _request(
            app,
            "POST",
            "/v1/charges",
            json=PaymentRequest(order_id="order-1", amount_cents=500, currency="USD").model_dump(
                mode="json"
            ),
            headers={
                "traceparent": parent.traceparent,
                "x-traceharbor-scenario": Scenario.PAYMENT_FAILURE.value,
            },
        )
    )

    assert response.status_code == 503
    assert response.json()["trace_id"] == parent.trace_id
    assert response.json()["parent_span_id"] == parent.span_id
    assert response.json()["status"] == StepStatus.FAILED.value
    assert len(sink.events) == 1
    assert sink.events[0].trace_id == parent.trace_id


def test_inventory_failure_is_an_expected_service_result() -> None:
    ids = DeterministicIdFactory("inventory-test")
    parent = ids.root()
    app = create_inventory_app(id_factory=ids)
    response = asyncio.run(
        _request(
            app,
            "POST",
            "/v1/reservations",
            json=InventoryRequest(order_id="order-1", item_id="item-1", quantity=1).model_dump(
                mode="json"
            ),
            headers={
                "traceparent": parent.traceparent,
                "x-traceharbor-scenario": Scenario.INVENTORY_FAILURE.value,
            },
        )
    )
    assert response.status_code == 409
    assert response.json()["status"] == StepStatus.FAILED.value


def test_malformed_traceparent_is_a_client_error_without_event() -> None:
    sink = CollectingEventSink()
    app = create_payment_app(event_sink=sink, sleep=_no_sleep)
    response = asyncio.run(
        _request(
            app,
            "POST",
            "/v1/charges",
            json={"order_id": "order-1", "amount_cents": 500, "currency": "USD"},
            headers={"traceparent": "invalid"},
        )
    )
    assert response.status_code == 400
    assert sink.events == []


def test_unknown_scenario_is_a_client_error() -> None:
    app = create_inventory_app()
    response = asyncio.run(
        _request(
            app,
            "POST",
            "/v1/reservations",
            json={"order_id": "order-1", "item_id": "item-1", "quantity": 1},
            headers={"x-traceharbor-scenario": "unknown"},
        )
    )
    assert response.status_code == 400


def test_gateway_rejects_broken_trace_lineage() -> None:
    downstream = FastAPI()

    @downstream.post("/v1/charges")
    async def bad_charge() -> dict[str, object]:
        return {
            "service": "payments",
            "status": "OK",
            "detail": "wrong trace",
            "trace_id": "9" * 32,
            "span_id": "8" * 16,
            "parent_span_id": "7" * 16,
            "simulated_delay_ms": 0,
        }

    async def exercise() -> None:
        parent = TraceContext(trace_id="1" * 32, span_id="2" * 16)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=downstream),
            base_url="http://downstream.test",
        ) as client:
            gateway = ClientPaymentGateway(client)
            with pytest.raises(ValueError, match="different trace ID"):
                await gateway.charge(
                    PaymentRequest(order_id="order-1", amount_cents=500, currency="USD"),
                    parent,
                    Scenario.HEALTHY,
                )

    asyncio.run(exercise())

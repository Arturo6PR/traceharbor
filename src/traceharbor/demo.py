"""In-process deterministic service topology used by the portfolio demonstration."""

from __future__ import annotations

import httpx

from traceharbor.contracts import CheckoutRequest, Scenario, ScenarioReport
from traceharbor.events import EventPublisher
from traceharbor.gateways import ClientInventoryGateway, ClientPaymentGateway
from traceharbor.services.inventory import create_inventory_app
from traceharbor.services.orders import create_orders_app
from traceharbor.services.payments import create_payment_app
from traceharbor.telemetry import CollectingEventSink
from traceharbor.tracecontext import DeterministicIdFactory

DEMO_REQUEST = CheckoutRequest(
    order_id="order-demo-001",
    item_id="signal-adapter",
    amount_cents=12_500,
    currency="USD",
    quantity=1,
)


async def _no_sleep(seconds: float) -> None:
    del seconds


async def run_demo(
    scenario: Scenario,
    *,
    seed: str = "traceharbor-phase1",
    event_publisher: EventPublisher | None = None,
) -> ScenarioReport:
    ids = DeterministicIdFactory(seed)
    sink = CollectingEventSink()
    payment_app = create_payment_app(id_factory=ids, event_sink=sink, sleep=_no_sleep)
    inventory_app = create_inventory_app(id_factory=ids, event_sink=sink)

    async with (
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=payment_app),
            base_url="http://payments.test",
        ) as payment_client,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=inventory_app),
            base_url="http://inventory.test",
        ) as inventory_client,
    ):
        orders_app = create_orders_app(
            ClientPaymentGateway(payment_client),
            ClientInventoryGateway(inventory_client),
            id_factory=ids,
            event_sink=sink,
            event_publisher=event_publisher,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=orders_app),
            base_url="http://orders.test",
        ) as orders_client:
            response = await orders_client.post(
                "/v1/orders",
                json=DEMO_REQUEST.model_dump(mode="json"),
                headers={"x-traceharbor-scenario": scenario.value},
            )

    if response.status_code not in {200, 424}:
        raise RuntimeError(f"demo topology returned unexpected HTTP {response.status_code}")
    report = ScenarioReport.model_validate(response.json())
    if any(step.trace_id != report.trace_id for step in report.steps):
        raise RuntimeError("demo topology produced inconsistent trace IDs")
    return report

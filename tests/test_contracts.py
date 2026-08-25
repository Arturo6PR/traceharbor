import pytest
from pydantic import ValidationError

from traceharbor.contracts import CheckoutRequest, ServiceStep, StepStatus


def test_checkout_request_is_strict() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        CheckoutRequest(
            order_id="order-1",
            item_id="item-1",
            amount_cents=500,
            currency="USD",
            quantity=1,
            unexpected=True,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("trace_id", "0" * 31),
        ("span_id", "not-hex-value"),
        ("parent_span_id", "abc"),
    ],
)
def test_service_step_rejects_invalid_trace_identifiers(field: str, value: str) -> None:
    payload = {
        "service": "orders",
        "status": StepStatus.OK,
        "detail": "order completed",
        "trace_id": "1" * 32,
        "span_id": "2" * 16,
        "parent_span_id": None,
    }
    payload[field] = value
    with pytest.raises(ValidationError):
        ServiceStep.model_validate(payload)


def test_checkout_request_rejects_nonpositive_amount() -> None:
    with pytest.raises(ValidationError):
        CheckoutRequest(order_id="order-1", item_id="item-1", amount_cents=0)

import pytest

from traceharbor.tracecontext import (
    DeterministicIdFactory,
    parse_traceparent,
    service_context,
)


def test_traceparent_round_trip_and_child_relationship() -> None:
    ids = DeterministicIdFactory("test-seed")
    root = ids.root()
    parsed = parse_traceparent(root.traceparent)
    child = service_context(root.traceparent, ids)

    assert parsed.trace_id == root.trace_id
    assert parsed.span_id == root.span_id
    assert child.trace_id == root.trace_id
    assert child.parent_span_id == root.span_id
    assert child.span_id != root.span_id


def test_deterministic_factory_repeats_for_same_seed() -> None:
    first = DeterministicIdFactory("repeatable")
    second = DeterministicIdFactory("repeatable")

    first_root = first.root()
    second_root = second.root()
    assert first_root == second_root
    assert first.child(first_root) == second.child(second_root)


@pytest.mark.parametrize(
    "value",
    [
        "malformed",
        "01-11111111111111111111111111111111-2222222222222222-01",
        "00-00000000000000000000000000000000-2222222222222222-01",
        "00-11111111111111111111111111111111-0000000000000000-01",
    ],
)
def test_invalid_traceparent_is_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        parse_traceparent(value)


def test_empty_deterministic_seed_is_rejected() -> None:
    with pytest.raises(ValueError, match="seed must not be empty"):
        DeterministicIdFactory("")

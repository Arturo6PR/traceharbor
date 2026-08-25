import asyncio
import base64
import io
import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from traceharbor.consumer import (
    BrokerRecord,
    ConsumerConfig,
    DeadLetterReason,
    DeadLetterRecord,
    EventProcessor,
    InMemoryProcessedEventStore,
    ProcessingDisposition,
    SQLiteProcessedEventStore,
)
from traceharbor.contracts import Scenario
from traceharbor.demo import run_demo
from traceharbor.events import (
    ORDER_DLQ_TOPIC,
    ORDER_EVENTS_TOPIC,
    CollectingEventPublisher,
    ConsoleEventPublisher,
    EventingConfig,
    EventsMode,
    OrderOutcomeEvent,
)
from traceharbor.observability import ObservabilityConfig, TelemetryMode, TelemetryRuntime

ROOT = Path(__file__).resolve().parents[1]


async def _record(scenario: Scenario = Scenario.HEALTHY) -> tuple[BrokerRecord, OrderOutcomeEvent]:
    publisher = CollectingEventPublisher()
    await run_demo(scenario, seed="event-test", event_publisher=publisher)
    assert len(publisher.events) == 1
    message = publisher.events[0]
    event = OrderOutcomeEvent.model_validate_json(message.value)
    return (
        BrokerRecord(
            topic=message.topic,
            partition=1,
            offset=42,
            key=message.key.encode("utf-8"),
            value=message.value,
            headers=message.headers,
        ),
        event,
    )


class ControlledHandler:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.calls = 0
        self.events: list[OrderOutcomeEvent] = []

    async def handle(self, event: OrderOutcomeEvent) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise TemporaryHandlerError("controlled failure")
        self.events.append(event)


class TemporaryHandlerError(RuntimeError):
    pass


class BrokenPublisher:
    async def publish(self, event) -> None:
        del event
        raise RuntimeError("broker unavailable")

    async def close(self) -> None:
        return None


def test_orders_publish_one_deterministic_versioned_event() -> None:
    first_record, first = asyncio.run(_record())
    second_record, second = asyncio.run(_record())

    assert first_record.topic == ORDER_EVENTS_TOPIC
    assert first_record.key == b"order-demo-001"
    assert first.event_schema_version == "1.0"
    assert first.event_type == "order.outcome.recorded"
    traceparent = dict(first_record.headers)["traceparent"].decode("ascii")
    assert first.trace_id == traceparent.split("-")[1]
    assert first == second
    assert first_record.value == second_record.value
    assert dict(first_record.headers)["traceharbor-event-schema"] == b"1.0"
    assert dict(first_record.headers)["traceparent"].startswith(b"00-")


@pytest.mark.parametrize(
    "scenario",
    [
        Scenario.HEALTHY,
        Scenario.PAYMENT_LATENCY,
        Scenario.PAYMENT_FAILURE,
        Scenario.INVENTORY_FAILURE,
    ],
)
def test_every_order_outcome_is_published(scenario: Scenario) -> None:
    _, event = asyncio.run(_record(scenario))

    assert event.scenario is scenario


def test_order_request_surfaces_event_delivery_failure() -> None:
    with pytest.raises(RuntimeError, match="broker unavailable"):
        asyncio.run(
            run_demo(
                Scenario.HEALTHY,
                seed="publish-failure",
                event_publisher=BrokenPublisher(),
            )
        )


def test_event_and_dead_letter_payloads_match_checked_in_schemas() -> None:
    record, _ = asyncio.run(_record())
    event_schema = json.loads(
        (ROOT / "docs" / "order-event-schema-v1.0.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(event_schema).validate(json.loads(record.value))

    publisher = CollectingEventPublisher()
    processor = EventProcessor(
        ControlledHandler(), InMemoryProcessedEventStore(), publisher, retry_base_delay_ms=0
    )
    asyncio.run(
        processor.process(
            BrokerRecord(
                topic=ORDER_EVENTS_TOPIC,
                partition=0,
                offset=7,
                key=None,
                value=b"not-json",
            )
        )
    )
    dead_letter_schema = json.loads(
        (ROOT / "docs" / "dead-letter-schema-v1.0.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(dead_letter_schema).validate(json.loads(publisher.events[0].value))


def test_processed_event_is_idempotently_skipped_on_redelivery() -> None:
    record, event = asyncio.run(_record())
    handler = ControlledHandler()
    store = InMemoryProcessedEventStore()
    dead_letters = CollectingEventPublisher()
    processor = EventProcessor(handler, store, dead_letters, retry_base_delay_ms=0)

    first = asyncio.run(processor.process(record))
    duplicate = asyncio.run(processor.process(record))

    assert first.disposition is ProcessingDisposition.PROCESSED
    assert first.attempts == 1
    assert duplicate.disposition is ProcessingDisposition.DUPLICATE
    assert duplicate.attempts == 0
    assert duplicate.event_id == event.event_id
    assert handler.calls == 1
    assert dead_letters.events == []


def test_handler_retries_with_deterministic_exponential_backoff() -> None:
    record, event = asyncio.run(_record())
    handler = ControlledHandler(failures=2)
    delays: list[float] = []

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    processor = EventProcessor(
        handler,
        InMemoryProcessedEventStore(),
        CollectingEventPublisher(),
        max_attempts=3,
        retry_base_delay_ms=100,
        sleep=record_sleep,
    )
    result = asyncio.run(processor.process(record))

    assert result.disposition is ProcessingDisposition.PROCESSED
    assert result.attempts == 3
    assert result.event_id == event.event_id
    assert delays == [0.1, 0.2]


def test_retry_backoff_is_capped_at_ten_seconds() -> None:
    record, _ = asyncio.run(_record())
    handler = ControlledHandler(failures=2)
    delays: list[float] = []

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    processor = EventProcessor(
        handler,
        InMemoryProcessedEventStore(),
        CollectingEventPublisher(),
        max_attempts=3,
        retry_base_delay_ms=6000,
        sleep=record_sleep,
    )
    assert asyncio.run(processor.process(record)).disposition is ProcessingDisposition.PROCESSED
    assert delays == [6.0, 10.0]


def test_consumer_continues_trace_and_exports_correlated_result() -> None:
    record, _ = asyncio.run(_record())
    stream = io.StringIO()
    runtime = TelemetryRuntime(
        ObservabilityConfig(
            service_name="order-consumer",
            mode=TelemetryMode.CONSOLE,
            metric_export_interval_ms=60_000,
        ),
        console_stream=stream,
    )
    processor = EventProcessor(
        ControlledHandler(),
        InMemoryProcessedEventStore(),
        CollectingEventPublisher(),
        retry_base_delay_ms=0,
        observability=runtime,
    )

    result = asyncio.run(processor.process(record))
    assert result.disposition is ProcessingDisposition.PROCESSED
    assert runtime.force_flush()
    runtime.shutdown()
    exported = stream.getvalue()
    trace_id = dict(record.headers)["traceparent"].decode("ascii").split("-")[1]
    assert "consume order.outcome.recorded" in exported
    assert f'"trace_id": "0x{trace_id}"' in exported
    assert "SpanKind.CONSUMER" in exported
    assert "traceharbor.events.processed" in exported
    assert "event_processed" in exported


def test_exhausted_handler_publishes_versioned_dead_letter() -> None:
    record, event = asyncio.run(_record())
    handler = ControlledHandler(failures=3)
    dead_letters = CollectingEventPublisher()
    processor = EventProcessor(
        handler,
        InMemoryProcessedEventStore(),
        dead_letters,
        max_attempts=3,
        retry_base_delay_ms=0,
    )

    result = asyncio.run(processor.process(record))
    dead_letter = DeadLetterRecord.model_validate_json(dead_letters.events[0].value)

    assert result.disposition is ProcessingDisposition.DEAD_LETTERED
    assert result.attempts == 3
    assert dead_letters.events[0].topic == ORDER_DLQ_TOPIC
    assert dead_letter.reason is DeadLetterReason.HANDLER_RETRIES_EXHAUSTED
    assert dead_letter.error_type == "TemporaryHandlerError"
    assert dead_letter.original_event_id == event.event_id
    assert base64.b64decode(dead_letter.original_payload_base64) == record.value
    assert (
        dict(dead_letters.events[0].headers)["traceparent"] == dict(record.headers)["traceparent"]
    )


def test_failed_dlq_publication_is_not_reported_as_terminal() -> None:
    record, _ = asyncio.run(_record())
    processor = EventProcessor(
        ControlledHandler(failures=1),
        InMemoryProcessedEventStore(),
        BrokenPublisher(),
        max_attempts=1,
        retry_base_delay_ms=0,
    )

    with pytest.raises(RuntimeError, match="broker unavailable"):
        asyncio.run(processor.process(record))


def test_malformed_event_goes_directly_to_dead_letter() -> None:
    dead_letters = CollectingEventPublisher()
    processor = EventProcessor(ControlledHandler(), InMemoryProcessedEventStore(), dead_letters)
    record = BrokerRecord(
        topic=ORDER_EVENTS_TOPIC,
        partition=0,
        offset=8,
        key=b"broken",
        value=b"",
    )

    first = asyncio.run(processor.process(record))
    first_dead_letter = dead_letters.events[0]
    second = asyncio.run(processor.process(record))

    assert first.disposition is ProcessingDisposition.DEAD_LETTERED
    assert first.attempts == 0
    assert second.dead_letter_id == first.dead_letter_id
    dead_letter = DeadLetterRecord.model_validate_json(first_dead_letter.value)
    assert dead_letter.reason is DeadLetterReason.INVALID_EVENT
    assert dead_letter.original_event_id is None
    assert dead_letter.original_payload_base64 == ""


def test_null_tombstone_goes_directly_to_dead_letter() -> None:
    dead_letters = CollectingEventPublisher()
    processor = EventProcessor(ControlledHandler(), InMemoryProcessedEventStore(), dead_letters)

    result = asyncio.run(
        processor.process(BrokerRecord(ORDER_EVENTS_TOPIC, 0, 9, b"order-1", None))
    )

    assert result.disposition is ProcessingDisposition.DEAD_LETTERED
    dead_letter = DeadLetterRecord.model_validate_json(dead_letters.events[0].value)
    assert dead_letter.reason is DeadLetterReason.INVALID_EVENT
    assert dead_letter.original_payload_base64 is None


@pytest.mark.parametrize("header", ["traceparent", "traceharbor-event-schema", "content-type"])
def test_missing_required_event_header_goes_to_dead_letter(header: str) -> None:
    record, _ = asyncio.run(_record())
    record = BrokerRecord(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        key=record.key,
        value=record.value,
        headers=tuple(item for item in record.headers if item[0] != header),
    )
    handler = ControlledHandler()
    dead_letters = CollectingEventPublisher()
    processor = EventProcessor(handler, InMemoryProcessedEventStore(), dead_letters)

    result = asyncio.run(processor.process(record))

    assert result.disposition is ProcessingDisposition.DEAD_LETTERED
    assert handler.calls == 0
    assert len(dead_letters.events) == 1


def test_mismatched_trace_header_goes_to_dead_letter() -> None:
    record, _ = asyncio.run(_record())
    headers = tuple(
        (name, b"00-99999999999999999999999999999999-8888888888888888-01")
        if name == "traceparent"
        else (name, value)
        for name, value in record.headers
    )
    record = BrokerRecord(
        record.topic, record.partition, record.offset, record.key, record.value, headers
    )
    handler = ControlledHandler()
    dead_letters = CollectingEventPublisher()

    result = asyncio.run(
        EventProcessor(handler, InMemoryProcessedEventStore(), dead_letters).process(record)
    )

    assert result.disposition is ProcessingDisposition.DEAD_LETTERED
    assert handler.calls == 0


def test_event_rejects_forged_identity_and_inconsistent_counts() -> None:
    _, event = asyncio.run(_record())
    payload = event.model_dump(mode="json")
    payload["event_id"] = "f" * 32
    with pytest.raises(ValidationError, match="event identity"):
        OrderOutcomeEvent.model_validate(payload)

    payload = event.model_dump(mode="json")
    payload["counts"]["failed"] = 1
    with pytest.raises(ValidationError, match="HEALTHY event counts"):
        OrderOutcomeEvent.model_validate(payload)


def test_sqlite_idempotency_survives_store_restart(tmp_path: Path) -> None:
    path = tmp_path / "consumer" / "events.sqlite3"
    first = SQLiteProcessedEventStore(path)
    first.mark_processed("a" * 32)
    first.close()

    second = SQLiteProcessedEventStore(path)
    try:
        assert second.is_processed("a" * 32)
        assert not second.is_processed("b" * 32)
    finally:
        second.close()


def test_console_publisher_is_stable_json() -> None:
    record, event = asyncio.run(_record())
    stream = io.StringIO()
    publisher = ConsoleEventPublisher(stream)
    message = asyncio.run(_published_message(record, event))

    asyncio.run(publisher.publish(message))
    rendered = json.loads(stream.getvalue())
    assert rendered["topic"] == ORDER_EVENTS_TOPIC
    assert rendered["key"] == event.order_id
    assert rendered["value"]["event_id"] == event.event_id


async def _published_message(record: BrokerRecord, event: OrderOutcomeEvent):
    del event
    from traceharbor.events import PublishedEvent

    return PublishedEvent(
        topic=record.topic,
        key=record.key.decode("utf-8"),
        value=record.value,
        headers=record.headers,
    )


def test_event_and_consumer_configuration_validation() -> None:
    assert EventingConfig.from_environment({}).mode is EventsMode.DISABLED
    assert (
        EventingConfig.from_environment(
            {
                "TRACEHARBOR_EVENTS_MODE": "kafka",
                "TRACEHARBOR_KAFKA_BOOTSTRAP_SERVERS": "one:9092,two:9092",
            }
        ).mode
        is EventsMode.KAFKA
    )
    assert (
        ConsumerConfig.from_environment(
            {
                "TRACEHARBOR_CONSUMER_MAX_ATTEMPTS": "4",
                "TRACEHARBOR_CONSUMER_RETRY_BASE_DELAY_MS": "25",
                "TRACEHARBOR_CONSUMER_POLL_TIMEOUT": "2",
            }
        ).max_attempts
        == 4
    )

    with pytest.raises(ValueError, match="EVENTS_MODE"):
        EventingConfig.from_environment({"TRACEHARBOR_EVENTS_MODE": "rabbitmq"})
    with pytest.raises(ValueError, match="host:port"):
        EventingConfig(bootstrap_servers="localhost:not-a-port")
    with pytest.raises(ValueError, match="numeric"):
        ConsumerConfig.from_environment({"TRACEHARBOR_CONSUMER_MAX_ATTEMPTS": "many"})


def test_local_event_stack_is_pinned_loopback_only_and_initializes_topics() -> None:
    compose = yaml.safe_load((ROOT / "compose.events.yaml").read_text(encoding="utf-8"))
    services = compose["services"]

    assert set(services) == {"console", "redpanda", "topic-init"}
    assert services["redpanda"]["image"].endswith(":v26.2.2")
    assert services["console"]["image"].endswith(":v3.10.0")
    for service in services.values():
        assert not service["image"].endswith(":latest")
        assert "no-new-privileges:true" in service["security_opt"]
        for port in service.get("ports", []):
            assert port.startswith("127.0.0.1:")
    topic_command = services["topic-init"]["command"][0]
    assert ORDER_EVENTS_TOPIC in topic_command
    assert ORDER_DLQ_TOPIC in topic_command

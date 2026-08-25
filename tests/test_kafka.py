import asyncio
import json

import pytest

from traceharbor.consumer import (
    BrokerRecord,
    EventProcessor,
    InMemoryProcessedEventStore,
)
from traceharbor.events import (
    ORDER_EVENTS_TOPIC,
    CollectingEventPublisher,
    EventingConfig,
    EventsMode,
    OrderOutcomeEvent,
    PublishedEvent,
    order_event_id,
)
from traceharbor.kafka import KafkaConsumerWorker, KafkaEventPublisher


class FakeProducer:
    def __init__(self, *, remaining: int = 0, delivery_error=None) -> None:
        self.remaining = remaining
        self.delivery_error = delivery_error
        self.messages = []
        self.flush_calls = []

    def produce(self, **kwargs) -> None:
        self.messages.append(kwargs)
        kwargs["on_delivery"](self.delivery_error, None)

    def flush(self, timeout: float) -> int:
        self.flush_calls.append(timeout)
        return self.remaining


class FakeMessage:
    def __init__(self, record: BrokerRecord, error=None) -> None:
        self.record = record
        self._error = error

    def error(self):
        return self._error

    def topic(self):
        return self.record.topic

    def partition(self):
        return self.record.partition

    def offset(self):
        return self.record.offset

    def key(self):
        return self.record.key

    def value(self):
        return self.record.value

    def headers(self):
        return list(self.record.headers)


class FakeConsumer:
    def __init__(self, messages) -> None:
        self.messages = list(messages)
        self.subscriptions = []
        self.commits = []
        self.closed = False

    def subscribe(self, topics) -> None:
        self.subscriptions.append(topics)

    def poll(self, timeout):
        del timeout
        return self.messages.pop(0) if self.messages else None

    def commit(self, **kwargs) -> None:
        self.commits.append(kwargs)

    def close(self) -> None:
        self.closed = True


class RecordingHandler:
    def __init__(self) -> None:
        self.events = []

    async def handle(self, event: OrderOutcomeEvent) -> None:
        self.events.append(event)


def _message() -> PublishedEvent:
    trace_id = "2" * 32
    value = json.dumps(
        {
            "counts": {"degraded": 0, "failed": 0, "ok": 3},
            "event_id": order_event_id("order-1", trace_id),
            "event_schema_version": "1.0",
            "event_type": "order.outcome.recorded",
            "order_id": "order-1",
            "outcome": "HEALTHY",
            "scenario": "healthy",
            "source": "traceharbor.orders",
            "trace_id": trace_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return PublishedEvent(
        topic=ORDER_EVENTS_TOPIC,
        key="order-1",
        value=value,
        headers=(
            ("content-type", b"application/json"),
            ("traceharbor-event-schema", b"1.0"),
            (
                "traceparent",
                b"00-22222222222222222222222222222222-3333333333333333-01",
            ),
        ),
    )


def test_kafka_publisher_uses_key_headers_idempotence_and_delivery_ack() -> None:
    producer = FakeProducer()
    publisher = KafkaEventPublisher(
        EventingConfig(mode=EventsMode.KAFKA, delivery_timeout_seconds=3), producer=producer
    )
    message = _message()

    asyncio.run(publisher.publish(message))

    assert producer.messages[0]["topic"] == ORDER_EVENTS_TOPIC
    assert producer.messages[0]["key"] == b"order-1"
    assert producer.messages[0]["headers"] == list(message.headers)
    assert producer.flush_calls == [3]


@pytest.mark.parametrize(
    ("producer", "message"),
    [
        (FakeProducer(remaining=1), "timed out"),
        (FakeProducer(delivery_error="broker rejected event"), "delivery failed"),
    ],
)
def test_kafka_publisher_surfaces_delivery_failures(producer, message: str) -> None:
    publisher = KafkaEventPublisher(EventingConfig(mode=EventsMode.KAFKA), producer=producer)

    with pytest.raises(RuntimeError, match=message):
        asyncio.run(publisher.publish(_message()))


def test_worker_commits_only_after_processing_and_closes_consumer() -> None:
    message = _message()
    record = BrokerRecord(
        topic=message.topic,
        partition=0,
        offset=9,
        key=message.key.encode("utf-8"),
        value=message.value,
        headers=message.headers,
    )
    broker_message = FakeMessage(record)
    consumer = FakeConsumer([broker_message])
    handler = RecordingHandler()
    processor = EventProcessor(
        handler,
        InMemoryProcessedEventStore(),
        CollectingEventPublisher(),
        retry_base_delay_ms=0,
    )

    count = KafkaConsumerWorker(consumer, processor, topic=ORDER_EVENTS_TOPIC).run(1)

    assert count == 1
    assert consumer.subscriptions == [[ORDER_EVENTS_TOPIC]]
    assert consumer.commits == [{"message": broker_message, "asynchronous": False}]
    assert consumer.closed
    assert len(handler.events) == 1


def test_worker_does_not_commit_when_processing_raises() -> None:
    message = _message()
    record = BrokerRecord(
        topic=message.topic,
        partition=0,
        offset=9,
        key=message.key.encode("utf-8"),
        value=message.value,
        headers=message.headers,
    )
    consumer = FakeConsumer([FakeMessage(record)])

    class BrokenProcessor:
        async def process(self, record) -> None:
            del record
            raise RuntimeError("storage unavailable")

    worker = KafkaConsumerWorker(consumer, BrokenProcessor(), topic=ORDER_EVENTS_TOPIC)

    with pytest.raises(RuntimeError, match="storage unavailable"):
        worker.run(1)
    assert consumer.commits == []
    assert consumer.closed


def test_worker_surfaces_broker_error_without_commit() -> None:
    record = BrokerRecord(ORDER_EVENTS_TOPIC, 0, 1, None, b"{}")
    consumer = FakeConsumer([FakeMessage(record, error="partition unavailable")])
    processor = EventProcessor(
        RecordingHandler(),
        InMemoryProcessedEventStore(),
        CollectingEventPublisher(),
        retry_base_delay_ms=0,
    )

    with pytest.raises(RuntimeError, match="partition unavailable"):
        KafkaConsumerWorker(consumer, processor, topic=ORDER_EVENTS_TOPIC).run(1)
    assert consumer.commits == []
    assert consumer.closed

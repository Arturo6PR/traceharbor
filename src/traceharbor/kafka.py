"""Confluent Kafka adapters for the Redpanda-compatible event boundaries."""

from __future__ import annotations

import asyncio
import os
from typing import Any

from traceharbor.consumer import (
    BrokerRecord,
    ConsumerConfig,
    EventProcessor,
    JsonLineOutcomeHandler,
    SQLiteProcessedEventStore,
)
from traceharbor.events import (
    ORDER_EVENTS_TOPIC,
    EventingConfig,
    EventsMode,
    PublishedEvent,
)
from traceharbor.observability import TelemetryRuntime


class KafkaEventPublisher:
    def __init__(self, config: EventingConfig, producer: Any | None = None) -> None:
        if producer is None:
            from confluent_kafka import Producer

            producer = Producer(
                {
                    "bootstrap.servers": config.bootstrap_servers,
                    "client.id": "traceharbor-producer-v1",
                    "enable.idempotence": True,
                    "acks": "all",
                }
            )
        self._producer = producer
        self._delivery_timeout_seconds = config.delivery_timeout_seconds

    async def publish(self, event: PublishedEvent) -> None:
        await asyncio.to_thread(self._publish_sync, event)

    async def close(self) -> None:
        try:
            remaining = await asyncio.to_thread(
                self._producer.flush, self._delivery_timeout_seconds
            )
        except Exception as exc:
            raise RuntimeError(f"Kafka publisher close failed: {type(exc).__name__}") from exc
        if remaining:
            raise RuntimeError(f"Kafka publisher closed with {remaining} undelivered message(s)")

    def _publish_sync(self, event: PublishedEvent) -> None:
        delivery_errors: list[str] = []

        def on_delivery(error, message) -> None:
            del message
            if error is not None:
                delivery_errors.append(str(error))

        try:
            self._producer.produce(
                topic=event.topic,
                key=event.key.encode("utf-8"),
                value=event.value,
                headers=list(event.headers),
                on_delivery=on_delivery,
            )
            remaining = self._producer.flush(self._delivery_timeout_seconds)
        except Exception as exc:
            raise RuntimeError(f"Kafka publish failed: {type(exc).__name__}") from exc
        if remaining:
            raise RuntimeError(f"Kafka delivery timed out with {remaining} queued message(s)")
        if delivery_errors:
            raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")


class KafkaConsumerWorker:
    def __init__(
        self,
        consumer: Any,
        processor: EventProcessor,
        *,
        topic: str,
        poll_timeout_seconds: float = 1.0,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._topic = topic
        self._poll_timeout_seconds = poll_timeout_seconds

    def run(self, max_messages: int | None = None) -> int:
        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be at least 1")
        processed = 0
        self._consumer.subscribe([self._topic])
        try:
            while max_messages is None or processed < max_messages:
                message = self._consumer.poll(self._poll_timeout_seconds)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    raise RuntimeError(f"Kafka consume failed: {error}")
                record = BrokerRecord(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    key=message.key(),
                    value=message.value(),
                    headers=tuple(message.headers() or ()),
                )
                asyncio.run(self._processor.process(record))
                self._consumer.commit(message=message, asynchronous=False)
                processed += 1
        finally:
            self._consumer.close()
        return processed


def run_live_order_consumer(max_messages: int | None = None) -> int:
    from confluent_kafka import Consumer

    config = ConsumerConfig.from_environment()
    raw_delivery_timeout = os.environ.get("TRACEHARBOR_KAFKA_DELIVERY_TIMEOUT", "5")
    try:
        delivery_timeout = float(raw_delivery_timeout)
    except ValueError as exc:
        raise ValueError("TRACEHARBOR_KAFKA_DELIVERY_TIMEOUT must be a number") from exc
    publisher = KafkaEventPublisher(
        EventingConfig(
            mode=EventsMode.KAFKA,
            bootstrap_servers=config.bootstrap_servers,
            delivery_timeout_seconds=delivery_timeout,
        )
    )
    store = SQLiteProcessedEventStore(config.state_path)
    telemetry = TelemetryRuntime.from_environment("order-consumer")
    processor = EventProcessor(
        JsonLineOutcomeHandler(),
        store,
        publisher,
        max_attempts=config.max_attempts,
        retry_base_delay_ms=config.retry_base_delay_ms,
        observability=telemetry,
    )
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.group_id,
                "client.id": "traceharbor-order-audit-v1",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )
        worker = KafkaConsumerWorker(
            consumer,
            processor,
            topic=ORDER_EVENTS_TOPIC,
            poll_timeout_seconds=config.poll_timeout_seconds,
        )
        return worker.run(max_messages)
    except (OSError, RuntimeError, ValueError):
        raise
    except Exception as exc:
        raise RuntimeError(f"Kafka consumer failed: {type(exc).__name__}") from exc
    finally:
        try:
            asyncio.run(publisher.close())
        finally:
            try:
                store.close()
            finally:
                telemetry.shutdown()

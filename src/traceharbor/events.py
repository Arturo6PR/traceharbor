"""Versioned order-event contracts and publisher boundaries."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import IO, Literal, Protocol

from pydantic import Field, model_validator

from traceharbor.contracts import Outcome, OutcomeCounts, Scenario, ScenarioReport, StrictModel

EVENT_SCHEMA_VERSION = "1.0"
ORDER_EVENTS_TOPIC = "traceharbor.orders.v1"
ORDER_DLQ_TOPIC = "traceharbor.orders.dlq.v1"


class EventsMode(StrEnum):
    DISABLED = "disabled"
    CONSOLE = "console"
    KAFKA = "kafka"


@dataclass(frozen=True, slots=True)
class EventingConfig:
    mode: EventsMode = EventsMode.DISABLED
    bootstrap_servers: str = "127.0.0.1:19092"
    delivery_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        validate_bootstrap_servers(self.bootstrap_servers)
        if not 0.1 <= self.delivery_timeout_seconds <= 60:
            raise ValueError("Kafka delivery timeout must be between 0.1 and 60 seconds")

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> EventingConfig:
        values = os.environ if environ is None else environ
        raw_mode = values.get("TRACEHARBOR_EVENTS_MODE", EventsMode.DISABLED.value)
        try:
            mode = EventsMode(raw_mode.lower())
        except ValueError as exc:
            supported = ", ".join(mode.value for mode in EventsMode)
            raise ValueError(f"TRACEHARBOR_EVENTS_MODE must be one of: {supported}") from exc
        raw_timeout = values.get("TRACEHARBOR_KAFKA_DELIVERY_TIMEOUT", "5")
        try:
            timeout = float(raw_timeout)
        except ValueError as exc:
            raise ValueError("TRACEHARBOR_KAFKA_DELIVERY_TIMEOUT must be a number") from exc
        return cls(
            mode=mode,
            bootstrap_servers=values.get("TRACEHARBOR_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"),
            delivery_timeout_seconds=timeout,
        )


class OrderOutcomeEvent(StrictModel):
    event_schema_version: Literal["1.0"] = EVENT_SCHEMA_VERSION
    event_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    event_type: Literal["order.outcome.recorded"] = "order.outcome.recorded"
    source: Literal["traceharbor.orders"] = "traceharbor.orders"
    order_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9._-]+$")
    scenario: Scenario
    outcome: Outcome
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    counts: OutcomeCounts

    @model_validator(mode="after")
    def validate_identity_and_outcome(self) -> OrderOutcomeEvent:
        if self.event_id != order_event_id(self.order_id, self.trace_id):
            raise ValueError("event_id does not match the event identity")
        if self.outcome is Outcome.HEALTHY and (
            self.counts.failed or self.counts.degraded or not self.counts.ok
        ):
            raise ValueError("HEALTHY event counts are inconsistent")
        if self.outcome is Outcome.DEGRADED and (self.counts.failed or not self.counts.degraded):
            raise ValueError("DEGRADED event counts are inconsistent")
        if self.outcome is Outcome.FAILED and not self.counts.failed:
            raise ValueError("FAILED event counts are inconsistent")
        return self

    @classmethod
    def from_report(cls, report: ScenarioReport) -> OrderOutcomeEvent:
        return cls(
            event_id=order_event_id(report.order_id, report.trace_id),
            order_id=report.order_id,
            scenario=report.scenario,
            outcome=report.outcome,
            trace_id=report.trace_id,
            counts=report.counts,
        )


@dataclass(frozen=True, slots=True)
class PublishedEvent:
    topic: str
    key: str
    value: bytes
    headers: tuple[tuple[str, bytes], ...]


class EventPublisher(Protocol):
    async def publish(self, event: PublishedEvent) -> None: ...

    async def close(self) -> None: ...


class NullEventPublisher:
    async def publish(self, event: PublishedEvent) -> None:
        del event

    async def close(self) -> None:
        return None


class CollectingEventPublisher:
    def __init__(self) -> None:
        self.events: list[PublishedEvent] = []

    async def publish(self, event: PublishedEvent) -> None:
        self.events.append(event)

    async def close(self) -> None:
        return None


class ConsoleEventPublisher:
    def __init__(self, stream: IO[str] | None = None) -> None:
        self._stream = stream or sys.stdout

    async def publish(self, event: PublishedEvent) -> None:
        rendered = {
            "headers": {name: value.decode("utf-8") for name, value in sorted(event.headers)},
            "key": event.key,
            "topic": event.topic,
            "value": json.loads(event.value),
        }
        self._stream.write(json.dumps(rendered, sort_keys=True, separators=(",", ":")) + "\n")

    async def close(self) -> None:
        return None


def order_event_message(report: ScenarioReport, traceparent: str) -> PublishedEvent:
    event = OrderOutcomeEvent.from_report(report)
    value = json.dumps(
        event.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return PublishedEvent(
        topic=ORDER_EVENTS_TOPIC,
        key=event.order_id,
        value=value,
        headers=(
            ("content-type", b"application/json"),
            ("traceparent", traceparent.encode("ascii")),
            ("traceharbor-event-schema", EVENT_SCHEMA_VERSION.encode("ascii")),
        ),
    )


def order_event_id(order_id: str, trace_id: str) -> str:
    identity = f"order.outcome.recorded:{order_id}:{trace_id}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]


def validate_bootstrap_servers(value: str) -> None:
    servers = [server.strip() for server in value.split(",")]
    if not servers:
        raise ValueError("Kafka bootstrap servers must be a comma-separated host:port list")
    for server in servers:
        host, separator, raw_port = server.rpartition(":")
        if (
            not separator
            or not host
            or any(character.isspace() for character in server)
            or not raw_port.isdigit()
            or not 1 <= int(raw_port) <= 65_535
        ):
            raise ValueError("Kafka bootstrap servers must be a comma-separated host:port list")


def create_event_publisher(config: EventingConfig) -> EventPublisher:
    if config.mode is EventsMode.DISABLED:
        return NullEventPublisher()
    if config.mode is EventsMode.CONSOLE:
        return ConsoleEventPublisher()
    from traceharbor.kafka import KafkaEventPublisher

    return KafkaEventPublisher(config)

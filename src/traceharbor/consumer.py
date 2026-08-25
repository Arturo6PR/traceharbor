"""Deterministic event processing, idempotency, retries, and dead-letter behavior."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import sqlite3
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import IO, Literal, Protocol

from pydantic import Field, ValidationError

from traceharbor.contracts import StrictModel
from traceharbor.events import (
    EVENT_SCHEMA_VERSION,
    ORDER_DLQ_TOPIC,
    EventPublisher,
    OrderOutcomeEvent,
    PublishedEvent,
    validate_bootstrap_servers,
)
from traceharbor.observability import TelemetryRuntime
from traceharbor.tracecontext import parse_traceparent

SleepFunction = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ConsumerConfig:
    bootstrap_servers: str = "127.0.0.1:19092"
    group_id: str = "traceharbor-order-audit-v1"
    max_attempts: int = 3
    retry_base_delay_ms: int = 100
    poll_timeout_seconds: float = 1.0
    state_path: Path = Path(".traceharbor/processed-events.sqlite3")

    def __post_init__(self) -> None:
        validate_bootstrap_servers(self.bootstrap_servers)
        if not self.group_id or any(character.isspace() for character in self.group_id):
            raise ValueError("Kafka consumer group must be a nonempty label without whitespace")
        if not 1 <= self.max_attempts <= 10:
            raise ValueError("consumer max attempts must be between 1 and 10")
        if not 0 <= self.retry_base_delay_ms <= 10_000:
            raise ValueError("consumer retry base delay must be between 0 and 10000 milliseconds")
        if not 0.1 <= self.poll_timeout_seconds <= 30:
            raise ValueError("consumer poll timeout must be between 0.1 and 30 seconds")

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> ConsumerConfig:
        values = os.environ if environ is None else environ
        try:
            max_attempts = int(values.get("TRACEHARBOR_CONSUMER_MAX_ATTEMPTS", "3"))
            retry_delay = int(values.get("TRACEHARBOR_CONSUMER_RETRY_BASE_DELAY_MS", "100"))
            poll_timeout = float(values.get("TRACEHARBOR_CONSUMER_POLL_TIMEOUT", "1"))
        except ValueError as exc:
            raise ValueError("consumer retry and poll settings must be numeric") from exc
        return cls(
            bootstrap_servers=values.get("TRACEHARBOR_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"),
            group_id=values.get("TRACEHARBOR_KAFKA_CONSUMER_GROUP", "traceharbor-order-audit-v1"),
            max_attempts=max_attempts,
            retry_base_delay_ms=retry_delay,
            poll_timeout_seconds=poll_timeout,
            state_path=Path(
                values.get(
                    "TRACEHARBOR_CONSUMER_STATE_PATH",
                    ".traceharbor/processed-events.sqlite3",
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class BrokerRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes | None], ...] = ()


class ProcessingDisposition(StrEnum):
    PROCESSED = "PROCESSED"
    DUPLICATE = "DUPLICATE"
    DEAD_LETTERED = "DEAD_LETTERED"


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    disposition: ProcessingDisposition
    attempts: int
    event_id: str | None = None
    dead_letter_id: str | None = None


class DeadLetterReason(StrEnum):
    INVALID_EVENT = "INVALID_EVENT"
    HANDLER_RETRIES_EXHAUSTED = "HANDLER_RETRIES_EXHAUSTED"


class DeadLetterRecord(StrictModel):
    dead_letter_schema_version: Literal["1.0"] = "1.0"
    dead_letter_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    reason: DeadLetterReason
    error_type: str = Field(min_length=1, max_length=100)
    attempts: int = Field(ge=0, le=10)
    original_topic: str = Field(min_length=1, max_length=249)
    original_partition: int = Field(ge=0)
    original_offset: int = Field(ge=0)
    original_key_base64: str | None = Field(default=None, max_length=2_000_000)
    original_event_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    original_payload_base64: str | None = Field(default=None, max_length=2_000_000)


class EventHandler(Protocol):
    async def handle(self, event: OrderOutcomeEvent) -> None: ...


class ProcessedEventStore(Protocol):
    def is_processed(self, event_id: str) -> bool: ...

    def mark_processed(self, event_id: str) -> None: ...

    def close(self) -> None: ...


class InMemoryProcessedEventStore:
    def __init__(self) -> None:
        self._event_ids: set[str] = set()

    def is_processed(self, event_id: str) -> bool:
        return event_id in self._event_ids

    def mark_processed(self, event_id: str) -> None:
        self._event_ids.add(event_id)

    def close(self) -> None:
        return None


class SQLiteProcessedEventStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path)
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS processed_events "
            "(event_id TEXT PRIMARY KEY CHECK(length(event_id) = 32))"
        )
        self._connection.commit()

    def is_processed(self, event_id: str) -> bool:
        row = self._connection.execute(
            "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return row is not None

    def mark_processed(self, event_id: str) -> None:
        self._connection.execute(
            "INSERT OR IGNORE INTO processed_events(event_id) VALUES (?)", (event_id,)
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()


class JsonLineOutcomeHandler:
    def __init__(self, stream: IO[str] | None = None) -> None:
        self._stream = stream or sys.stdout

    async def handle(self, event: OrderOutcomeEvent) -> None:
        output = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "order_id": event.order_id,
            "outcome": event.outcome.value,
            "trace_id": event.trace_id,
        }
        self._stream.write(json.dumps(output, sort_keys=True, separators=(",", ":")) + "\n")


class EventProcessor:
    def __init__(
        self,
        handler: EventHandler,
        store: ProcessedEventStore,
        dead_letter_publisher: EventPublisher,
        *,
        max_attempts: int = 3,
        retry_base_delay_ms: int = 100,
        sleep: SleepFunction = asyncio.sleep,
        observability: TelemetryRuntime | None = None,
    ) -> None:
        if not 1 <= max_attempts <= 10:
            raise ValueError("max_attempts must be between 1 and 10")
        if not 0 <= retry_base_delay_ms <= 10_000:
            raise ValueError("retry_base_delay_ms must be between 0 and 10000")
        self._handler = handler
        self._store = store
        self._dead_letter_publisher = dead_letter_publisher
        self._max_attempts = max_attempts
        self._retry_base_delay_ms = retry_base_delay_ms
        self._sleep = sleep
        self._observability = observability

    async def process(self, record: BrokerRecord) -> ProcessingResult:
        try:
            if record.value is None:
                raise ValueError("event payload must not be null")
            event = OrderOutcomeEvent.model_validate_json(record.value)
            _validate_event_headers(record, event)
        except (ValidationError, ValueError) as exc:
            dead_letter = _dead_letter(
                record,
                DeadLetterReason.INVALID_EVENT,
                type(exc).__name__,
                attempts=0,
            )
            await self._dead_letter_publisher.publish(_dead_letter_message(dead_letter, record))
            return ProcessingResult(
                disposition=ProcessingDisposition.DEAD_LETTERED,
                attempts=0,
                dead_letter_id=dead_letter.dead_letter_id,
            )

        traceparent = _required_header(record, "traceparent")
        if self._observability is None:
            return await self._process_valid(record, event)
        with self._observability.consume_event_span(traceparent, event.event_id):
            result = await self._process_valid(record, event)
            self._observability.record_event_result(result.disposition.value, result.attempts)
            return result

    async def _process_valid(
        self, record: BrokerRecord, event: OrderOutcomeEvent
    ) -> ProcessingResult:
        if self._store.is_processed(event.event_id):
            return ProcessingResult(
                disposition=ProcessingDisposition.DUPLICATE,
                attempts=0,
                event_id=event.event_id,
            )

        last_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                await self._handler.handle(event)
                self._store.mark_processed(event.event_id)
                return ProcessingResult(
                    disposition=ProcessingDisposition.PROCESSED,
                    attempts=attempt,
                    event_id=event.event_id,
                )
            except Exception as exc:  # The boundary intentionally retries handler failures.
                last_error = exc
                if attempt < self._max_attempts:
                    delay_ms = min(self._retry_base_delay_ms * (2 ** (attempt - 1)), 10_000)
                    await self._sleep(delay_ms / 1000)

        dead_letter = _dead_letter(
            record,
            DeadLetterReason.HANDLER_RETRIES_EXHAUSTED,
            type(last_error).__name__,
            attempts=self._max_attempts,
            original_event_id=event.event_id,
        )
        await self._dead_letter_publisher.publish(_dead_letter_message(dead_letter, record))
        return ProcessingResult(
            disposition=ProcessingDisposition.DEAD_LETTERED,
            attempts=self._max_attempts,
            event_id=event.event_id,
            dead_letter_id=dead_letter.dead_letter_id,
        )


def _dead_letter(
    record: BrokerRecord,
    reason: DeadLetterReason,
    error_type: str,
    *,
    attempts: int,
    original_event_id: str | None = None,
) -> DeadLetterRecord:
    payload_identity = b"<null>" if record.value is None else b"<bytes>" + record.value
    identity = (
        record.topic.encode("utf-8")
        + b":"
        + str(record.partition).encode("ascii")
        + b":"
        + str(record.offset).encode("ascii")
        + b":"
        + payload_identity
    )
    key = base64.b64encode(record.key).decode("ascii") if record.key is not None else None
    return DeadLetterRecord(
        dead_letter_id=hashlib.sha256(identity).hexdigest()[:32],
        reason=reason,
        error_type=error_type,
        attempts=attempts,
        original_topic=record.topic,
        original_partition=record.partition,
        original_offset=record.offset,
        original_key_base64=key,
        original_event_id=original_event_id,
        original_payload_base64=(
            base64.b64encode(record.value).decode("ascii") if record.value is not None else None
        ),
    )


def _validate_event_headers(record: BrokerRecord, event: OrderOutcomeEvent) -> None:
    content_type = _required_header(record, "content-type")
    schema_version = _required_header(record, "traceharbor-event-schema")
    traceparent = _required_header(record, "traceparent")
    if content_type != "application/json":
        raise ValueError("event content-type header must be application/json")
    if schema_version != EVENT_SCHEMA_VERSION:
        raise ValueError("event schema header does not match the supported version")
    if parse_traceparent(traceparent).trace_id != event.trace_id:
        raise ValueError("event traceparent does not match the payload trace ID")


def _required_header(record: BrokerRecord, name: str) -> str:
    values = [value for header_name, value in record.headers if header_name == name]
    if len(values) != 1:
        raise ValueError(f"event requires exactly one {name} header")
    if values[0] is None:
        raise ValueError(f"event {name} header must not be null")
    try:
        return values[0].decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError(f"event {name} header must be ASCII") from exc


def _dead_letter_message(dead_letter: DeadLetterRecord, source: BrokerRecord) -> PublishedEvent:
    value = json.dumps(
        dead_letter.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    try:
        traceparent = _required_header(source, "traceparent")
        parse_traceparent(traceparent)
        trace_header = (("traceparent", traceparent.encode("ascii")),)
    except ValueError:
        trace_header = ()
    return PublishedEvent(
        topic=ORDER_DLQ_TOPIC,
        key=dead_letter.original_event_id or dead_letter.dead_letter_id,
        value=value,
        headers=(
            ("content-type", b"application/json"),
            ("traceharbor-dead-letter-schema", b"1.0"),
            *trace_header,
        ),
    )

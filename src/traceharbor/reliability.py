"""Deterministic local recovery verification."""

from __future__ import annotations

import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

from pydantic import model_validator

from traceharbor.consumer import (
    BrokerRecord,
    EventProcessor,
    JsonLineOutcomeHandler,
    ProcessingDisposition,
    SQLiteProcessedEventStore,
)
from traceharbor.contracts import Scenario, StrictModel
from traceharbor.demo import run_demo
from traceharbor.events import CollectingEventPublisher

RECOVERY_REPORT_SCHEMA_VERSION = "1.0"


class ConsumerRecoveryReport(StrictModel):
    recovery_report_schema_version: Literal["1.0"] = RECOVERY_REPORT_SCHEMA_VERSION
    check: Literal["consumer_restart_deduplication"] = "consumer_restart_deduplication"
    first_processing: ProcessingDisposition
    after_restart: ProcessingDisposition
    handler_invocations: int
    dead_letter_events: int
    passed: bool

    @model_validator(mode="after")
    def validate_decision(self) -> ConsumerRecoveryReport:
        expected = (
            self.first_processing is ProcessingDisposition.PROCESSED
            and self.after_restart is ProcessingDisposition.DUPLICATE
            and self.handler_invocations == 1
            and self.dead_letter_events == 0
        )
        if self.passed is not expected:
            raise ValueError("recovery decision does not match the observed behavior")
        return self


class _CountingHandler(JsonLineOutcomeHandler):
    def __init__(self) -> None:
        super().__init__(io.StringIO())
        self.invocations = 0

    async def handle(self, event) -> None:
        self.invocations += 1
        await super().handle(event)


async def verify_consumer_recovery() -> ConsumerRecoveryReport:
    event_publisher = CollectingEventPublisher()
    await run_demo(
        Scenario.HEALTHY,
        seed="consumer-recovery-v1",
        event_publisher=event_publisher,
    )
    message = event_publisher.events[0]
    record = BrokerRecord(
        topic=message.topic,
        partition=0,
        offset=1,
        key=message.key.encode("utf-8"),
        value=message.value,
        headers=message.headers,
    )
    dead_letters = CollectingEventPublisher()
    handler = _CountingHandler()

    with TemporaryDirectory(prefix="traceharbor-recovery-") as temporary:
        state_path = Path(temporary) / "processed.sqlite3"
        first_store = SQLiteProcessedEventStore(state_path)
        try:
            first = await EventProcessor(handler, first_store, dead_letters).process(record)
        finally:
            first_store.close()

        restarted_store = SQLiteProcessedEventStore(state_path)
        try:
            restarted = await EventProcessor(handler, restarted_store, dead_letters).process(record)
        finally:
            restarted_store.close()

    passed = (
        first.disposition is ProcessingDisposition.PROCESSED
        and restarted.disposition is ProcessingDisposition.DUPLICATE
        and handler.invocations == 1
        and not dead_letters.events
    )
    return ConsumerRecoveryReport(
        first_processing=first.disposition,
        after_restart=restarted.disposition,
        handler_invocations=handler.invocations,
        dead_letter_events=len(dead_letters.events),
        passed=passed,
    )


def render_recovery_json(report: ConsumerRecoveryReport) -> str:
    return (
        json.dumps(
            report.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )


def render_recovery_text(report: ConsumerRecoveryReport) -> str:
    return "\n".join(
        (
            "TraceHarbor consumer recovery verification",
            f"Decision: {'PASS' if report.passed else 'FAIL'}",
            f"First processing: {report.first_processing.value}",
            f"After restart: {report.after_restart.value}",
            f"Handler invocations: {report.handler_invocations}",
            f"Dead-letter events: {report.dead_letter_events}",
            "",
        )
    )

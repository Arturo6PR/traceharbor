"""Structured event sinks kept independent from service behavior."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Protocol, TextIO

from traceharbor.contracts import Scenario, ServiceStep, TelemetryEvent


class EventSink(Protocol):
    def emit(self, event: TelemetryEvent) -> None: ...


class NullEventSink:
    def emit(self, event: TelemetryEvent) -> None:
        del event


@dataclass(slots=True)
class CollectingEventSink:
    events: list[TelemetryEvent] = field(default_factory=list)

    def emit(self, event: TelemetryEvent) -> None:
        self.events.append(event)


class JsonLineEventSink:
    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream or sys.stdout

    def emit(self, event: TelemetryEvent) -> None:
        payload = event.model_dump(mode="json")
        self._stream.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
        self._stream.flush()


def emit_step(sink: EventSink, scenario: Scenario, step: ServiceStep) -> None:
    sink.emit(
        TelemetryEvent(
            service=step.service,
            scenario=scenario,
            status=step.status,
            trace_id=step.trace_id,
            span_id=step.span_id,
        )
    )

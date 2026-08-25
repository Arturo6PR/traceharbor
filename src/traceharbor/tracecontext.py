"""Minimal W3C trace-context handling for the Phase 1 service boundary."""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass
from typing import Protocol

TRACEPARENT_PATTERN = re.compile(
    r"^(?P<version>[0-9a-f]{2})-(?P<trace_id>[0-9a-f]{32})-"
    r"(?P<span_id>[0-9a-f]{16})-(?P<flags>[0-9a-f]{2})$"
)


@dataclass(frozen=True, slots=True)
class TraceContext:
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    sampled: bool = True

    @property
    def traceparent(self) -> str:
        flags = "01" if self.sampled else "00"
        return f"00-{self.trace_id}-{self.span_id}-{flags}"


class IdFactory(Protocol):
    def root(self) -> TraceContext: ...

    def child(self, parent: TraceContext) -> TraceContext: ...


class RandomIdFactory:
    def root(self) -> TraceContext:
        return TraceContext(trace_id=_nonzero_token(16), span_id=_nonzero_token(8))

    def child(self, parent: TraceContext) -> TraceContext:
        return TraceContext(
            trace_id=parent.trace_id,
            span_id=_nonzero_token(8),
            parent_span_id=parent.span_id,
            sampled=parent.sampled,
        )


class DeterministicIdFactory:
    """Content-derived IDs make checked-in demonstrations byte-repeatable."""

    def __init__(self, seed: str) -> None:
        if not seed:
            raise ValueError("seed must not be empty")
        self._seed = seed
        self._counter = 0

    def _next(self, byte_count: int) -> str:
        self._counter += 1
        value = hashlib.sha256(f"{self._seed}:{self._counter}".encode()).hexdigest()
        token = value[: byte_count * 2]
        return token if set(token) != {"0"} else ("1" + token[1:])

    def root(self) -> TraceContext:
        return TraceContext(trace_id=self._next(16), span_id=self._next(8))

    def child(self, parent: TraceContext) -> TraceContext:
        return TraceContext(
            trace_id=parent.trace_id,
            span_id=self._next(8),
            parent_span_id=parent.span_id,
            sampled=parent.sampled,
        )


def parse_traceparent(value: str) -> TraceContext:
    match = TRACEPARENT_PATTERN.fullmatch(value.strip().lower())
    if match is None:
        raise ValueError("traceparent must use the W3C 00-<trace-id>-<span-id>-<flags> form")
    if match["version"] != "00":
        raise ValueError("only W3C trace-context version 00 is supported in Phase 1")
    if set(match["trace_id"]) == {"0"} or set(match["span_id"]) == {"0"}:
        raise ValueError("trace and span IDs must not be all zeroes")
    return TraceContext(
        trace_id=match["trace_id"],
        span_id=match["span_id"],
        sampled=bool(int(match["flags"], 16) & 1),
    )


def service_context(value: str | None, id_factory: IdFactory) -> TraceContext:
    if value is None:
        return id_factory.root()
    return id_factory.child(parse_traceparent(value))


def _nonzero_token(byte_count: int) -> str:
    token = secrets.token_hex(byte_count)
    return token if set(token) != {"0"} else ("1" + token[1:])

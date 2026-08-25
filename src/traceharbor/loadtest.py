"""Bounded concurrent release-load gate for a live Orders service."""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlparse

import httpx
from pydantic import Field, ValidationError, model_validator

from traceharbor.contracts import Scenario, ScenarioReport, StrictModel

LOAD_REPORT_SCHEMA_VERSION = "1.0"


class LoadStatusCount(StrictModel):
    status: str = Field(pattern=r"^(?:[1-5][0-9]{2}|INVALID_RESPONSE|TRANSPORT_ERROR)$")
    count: int = Field(ge=1)


class LoadLatencySummary(StrictModel):
    minimum_ms: float = Field(ge=0)
    median_ms: float = Field(ge=0)
    p95_ms: float = Field(ge=0)
    maximum_ms: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_order(self) -> LoadLatencySummary:
        if not self.minimum_ms <= self.median_ms <= self.p95_ms <= self.maximum_ms:
            raise ValueError("load latency summary must be ordered")
        return self


class LoadThresholds(StrictModel):
    maximum_error_rate: float = Field(ge=0, le=1)
    maximum_p95_ms: float = Field(gt=0, le=60_000)


class LoadTestReport(StrictModel):
    load_report_schema_version: Literal["1.0"] = LOAD_REPORT_SCHEMA_VERSION
    target: str
    scenario: Scenario
    requested: int = Field(ge=1)
    concurrency: int = Field(ge=1)
    succeeded: int = Field(ge=0)
    failed: int = Field(ge=0)
    error_rate: float = Field(ge=0, le=1)
    status_counts: tuple[LoadStatusCount, ...]
    latency: LoadLatencySummary
    thresholds: LoadThresholds
    passed: bool

    @model_validator(mode="after")
    def validate_totals(self) -> LoadTestReport:
        if self.concurrency > self.requested:
            raise ValueError("load concurrency must not exceed requested count")
        if self.succeeded + self.failed != self.requested:
            raise ValueError("load result counts must equal requested count")
        if sum(bucket.count for bucket in self.status_counts) != self.requested:
            raise ValueError("load status counts must equal requested count")
        statuses = [bucket.status for bucket in self.status_counts]
        if statuses != sorted(set(statuses)):
            raise ValueError("load status counts must be uniquely and deterministically ordered")
        expected_rate = round(self.failed / self.requested, 6)
        if self.error_rate != expected_rate:
            raise ValueError("load error rate does not match result counts")
        expected_passed = (
            self.error_rate <= self.thresholds.maximum_error_rate
            and self.latency.p95_ms <= self.thresholds.maximum_p95_ms
        )
        if self.passed is not expected_passed:
            raise ValueError("load gate decision does not match its thresholds")
        return self


@dataclass(frozen=True, slots=True)
class LoadTestConfig:
    target: str = "http://127.0.0.1:8001"
    requests: int = 100
    concurrency: int = 10
    scenario: Scenario = Scenario.HEALTHY
    timeout_seconds: float = 5.0
    maximum_error_rate: float = 0.01
    maximum_p95_ms: float = 500.0

    def __post_init__(self) -> None:
        parsed = urlparse(self.target)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.path not in {"", "/"}
            or parsed.params
            or parsed.query
            or parsed.fragment
            or parsed.username
            or parsed.password
        ):
            raise ValueError("load target must be an http(s) origin without credentials or a path")
        if not 1 <= self.requests <= 100_000:
            raise ValueError("load requests must be between 1 and 100000")
        if not 1 <= self.concurrency <= min(self.requests, 1_000):
            raise ValueError("load concurrency must be between 1 and the request count")
        if not 0.1 <= self.timeout_seconds <= 60:
            raise ValueError("load timeout must be between 0.1 and 60 seconds")
        if not 0 <= self.maximum_error_rate <= 1:
            raise ValueError("maximum error rate must be between 0 and 1")
        if not 1 <= self.maximum_p95_ms <= 60_000:
            raise ValueError("maximum p95 must be between 1 and 60000 milliseconds")


@dataclass(frozen=True, slots=True)
class _RequestResult:
    status: str
    succeeded: bool
    latency_ms: float


async def run_load_test(
    config: LoadTestConfig,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    clock: Callable[[], float] = time.perf_counter,
) -> LoadTestReport:
    semaphore = asyncio.Semaphore(config.concurrency)
    limits = httpx.Limits(
        max_connections=config.concurrency,
        max_keepalive_connections=config.concurrency,
    )
    async with httpx.AsyncClient(
        base_url=config.target.rstrip("/"),
        timeout=config.timeout_seconds,
        transport=transport,
        limits=limits,
    ) as client:

        async def send(index: int) -> _RequestResult:
            async with semaphore:
                started = clock()
                order_id = f"load-{index:06d}"
                try:
                    response = await client.post(
                        "/v1/orders",
                        headers={"x-traceharbor-scenario": config.scenario.value},
                        json={
                            "order_id": order_id,
                            "item_id": "signal-adapter",
                            "amount_cents": 12_500,
                            "currency": "USD",
                            "quantity": 1,
                        },
                    )
                    status = str(response.status_code)
                    succeeded = 200 <= response.status_code < 300
                    if succeeded:
                        try:
                            report = ScenarioReport.model_validate_json(response.content)
                        except ValidationError:
                            status = "INVALID_RESPONSE"
                            succeeded = False
                        else:
                            if (
                                report.order_id != order_id
                                or report.scenario is not config.scenario
                            ):
                                status = "INVALID_RESPONSE"
                                succeeded = False
                except httpx.RequestError:
                    status = "TRANSPORT_ERROR"
                    succeeded = False
                elapsed_ms = round(max(0.0, (clock() - started) * 1_000), 3)
                return _RequestResult(status, succeeded, elapsed_ms)

        results = await asyncio.gather(*(send(index) for index in range(1, config.requests + 1)))

    succeeded = sum(result.succeeded for result in results)
    failed = config.requests - succeeded
    latencies = sorted(result.latency_ms for result in results)
    status_counts = Counter(result.status for result in results)
    latency = LoadLatencySummary(
        minimum_ms=latencies[0],
        median_ms=_percentile(latencies, 50),
        p95_ms=_percentile(latencies, 95),
        maximum_ms=latencies[-1],
    )
    thresholds = LoadThresholds(
        maximum_error_rate=config.maximum_error_rate,
        maximum_p95_ms=config.maximum_p95_ms,
    )
    error_rate = round(failed / config.requests, 6)
    return LoadTestReport(
        target=config.target.rstrip("/"),
        scenario=config.scenario,
        requested=config.requests,
        concurrency=config.concurrency,
        succeeded=succeeded,
        failed=failed,
        error_rate=error_rate,
        status_counts=tuple(
            LoadStatusCount(status=status, count=count)
            for status, count in sorted(status_counts.items())
        ),
        latency=latency,
        thresholds=thresholds,
        passed=(
            error_rate <= thresholds.maximum_error_rate
            and latency.p95_ms <= thresholds.maximum_p95_ms
        ),
    )


def _percentile(ordered: list[float], percentile: int) -> float:
    index = max(0, math.ceil((percentile / 100) * len(ordered)) - 1)
    return ordered[index]


def render_load_json(report: LoadTestReport) -> str:
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


def render_load_text(report: LoadTestReport) -> str:
    decision = "PASS" if report.passed else "FAIL"
    statuses = ", ".join(f"{item.status}={item.count}" for item in report.status_counts)
    return "\n".join(
        (
            "TraceHarbor live load gate",
            f"Decision: {decision}",
            f"Target: {report.target}",
            f"Scenario: {report.scenario.value}",
            f"Requests: {report.requested} (concurrency={report.concurrency})",
            f"Results: succeeded={report.succeeded}, failed={report.failed}, statuses={statuses}",
            (
                f"Error rate: {report.error_rate:.2%} "
                f"(maximum={report.thresholds.maximum_error_rate:.2%})"
            ),
            (
                f"Latency p95: {report.latency.p95_ms:.3f} ms "
                f"(maximum={report.thresholds.maximum_p95_ms:.3f} ms)"
            ),
            "",
        )
    )

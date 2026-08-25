from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import jsonschema
import pytest
import yaml

from traceharbor.loadtest import (
    LoadTestConfig,
    render_load_json,
    render_load_text,
    run_load_test,
)
from traceharbor.reliability import (
    render_recovery_json,
    verify_consumer_recovery,
)

ROOT = Path(__file__).parents[1]


class IncrementingClock:
    def __init__(self, increment_seconds: float = 0.001) -> None:
        self._value = 0.0
        self._increment = increment_seconds

    def __call__(self) -> float:
        current = self._value
        self._value += self._increment
        return current


def _healthy_report(order_id: str) -> dict:
    trace_id = "1" * 32
    order_span_id = "2" * 16
    return {
        "report_schema_version": "1.0",
        "scenario": "healthy",
        "outcome": "HEALTHY",
        "order_id": order_id,
        "trace_id": trace_id,
        "counts": {"ok": 3, "degraded": 0, "failed": 0},
        "steps": [
            {
                "service": "orders",
                "status": "OK",
                "detail": "order completed",
                "trace_id": trace_id,
                "span_id": order_span_id,
                "parent_span_id": None,
                "simulated_delay_ms": 0,
            },
            {
                "service": "payments",
                "status": "OK",
                "detail": "payment authorized",
                "trace_id": trace_id,
                "span_id": "3" * 16,
                "parent_span_id": order_span_id,
                "simulated_delay_ms": 0,
            },
            {
                "service": "inventory",
                "status": "OK",
                "detail": "inventory reserved",
                "trace_id": trace_id,
                "span_id": "4" * 16,
                "parent_span_id": order_span_id,
                "simulated_delay_ms": 0,
            },
        ],
    }


def test_live_load_gate_passes_and_has_stable_status_order() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["order_id"].startswith("load-")
        assert request.headers["x-traceharbor-scenario"] == "healthy"
        return httpx.Response(200, json=_healthy_report(body["order_id"]))

    report = asyncio.run(
        run_load_test(
            LoadTestConfig(requests=12, concurrency=3, maximum_error_rate=0),
            transport=httpx.MockTransport(handler),
            clock=IncrementingClock(),
        )
    )

    assert report.passed
    assert report.succeeded == 12
    assert report.failed == 0
    assert [(bucket.status, bucket.count) for bucket in report.status_counts] == [("200", 12)]
    assert report.latency.minimum_ms == report.latency.p95_ms == 1.0
    assert "Decision: PASS" in render_load_text(report)


def test_live_load_gate_counts_http_and_transport_failures() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        index = int(json.loads(request.content)["order_id"].rsplit("-", 1)[1])
        if index == 3:
            raise httpx.ConnectError("offline", request=request)
        if index % 2 == 0:
            return httpx.Response(503)
        return httpx.Response(200, json=_healthy_report(f"load-{index:06d}"))

    report = asyncio.run(
        run_load_test(
            LoadTestConfig(
                requests=4,
                concurrency=1,
                maximum_error_rate=0.25,
                maximum_p95_ms=10,
            ),
            transport=httpx.MockTransport(handler),
            clock=IncrementingClock(),
        )
    )

    assert not report.passed
    assert report.succeeded == 1
    assert report.failed == 3
    assert report.error_rate == 0.75
    assert [(bucket.status, bucket.count) for bucket in report.status_counts] == [
        ("200", 1),
        ("503", 2),
        ("TRANSPORT_ERROR", 1),
    ]


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"target": "localhost:8001"}, "http"),
        ({"target": "http://user:secret@localhost:8001"}, "credentials"),
        ({"target": "http://localhost:8001/path"}, "without credentials or a path"),
        ({"requests": 0}, "between 1 and 100000"),
        ({"requests": 2, "concurrency": 3}, "between 1 and the request count"),
        ({"timeout_seconds": 0}, "between 0.1 and 60"),
        ({"maximum_error_rate": 2}, "between 0 and 1"),
        ({"maximum_p95_ms": 0}, "between 1 and 60000"),
    ],
)
def test_invalid_load_configuration_is_rejected(values: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        LoadTestConfig(**values)


def test_load_report_matches_the_versioned_schema() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        return httpx.Response(200, json=_healthy_report(body["order_id"]))

    report = asyncio.run(
        run_load_test(
            LoadTestConfig(requests=2, concurrency=1),
            transport=httpx.MockTransport(handler),
            clock=IncrementingClock(),
        )
    )
    document = json.loads(render_load_json(report))
    schema = json.loads((ROOT / "docs" / "load-report-schema-v1.0.json").read_text())

    jsonschema.Draft202012Validator(schema).validate(document)
    assert document["load_report_schema_version"] == "1.0"


def test_invalid_success_response_counts_as_a_failed_request() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(200, json={"outcome": "HEALTHY"})

    report = asyncio.run(
        run_load_test(
            LoadTestConfig(requests=1, concurrency=1),
            transport=httpx.MockTransport(handler),
            clock=IncrementingClock(),
        )
    )

    assert not report.passed
    assert report.status_counts[0].status == "INVALID_RESPONSE"


def test_consumer_recovery_is_deterministic_and_matches_schema() -> None:
    first = asyncio.run(verify_consumer_recovery())
    second = asyncio.run(verify_consumer_recovery())
    first_json = render_recovery_json(first)

    assert first.passed
    assert first.first_processing.value == "PROCESSED"
    assert first.after_restart.value == "DUPLICATE"
    assert first.handler_invocations == 1
    assert first.dead_letter_events == 0
    assert first_json == render_recovery_json(second)

    schema = json.loads((ROOT / "docs" / "recovery-report-schema-v1.0.json").read_text())
    jsonschema.Draft202012Validator(schema).validate(json.loads(first_json))


def test_slo_recording_rules_alerts_and_prometheus_wiring() -> None:
    observability = ROOT / "observability"
    prometheus = yaml.safe_load((observability / "prometheus.yaml").read_text())
    rules_document = yaml.safe_load((observability / "prometheus-rules.yaml").read_text())
    rules = [rule for group in rules_document["groups"] for rule in group["rules"]]

    assert prometheus["rule_files"] == ["/etc/prometheus/prometheus-rules.yaml"]
    assert {rule["record"] for rule in rules if "record" in rule} == {
        "traceharbor:orders_error_ratio:rate5m",
        "traceharbor:orders_error_ratio:rate30m",
        "traceharbor:orders_error_ratio:rate1h",
        "traceharbor:orders_error_ratio:rate6h",
        "traceharbor:consumer_dead_letter_ratio:rate5m",
    }
    assert {rule["alert"] for rule in rules if "alert" in rule} == {
        "TraceHarborOrdersAvailabilityFastBurn",
        "TraceHarborOrdersAvailabilitySlowBurn",
        "TraceHarborConsumerDeadLetterRatioHigh",
        "TraceHarborTelemetryCollectorUnavailable",
    }
    fast_burn = next(
        rule for rule in rules if rule.get("alert") == "TraceHarborOrdersAvailabilityFastBurn"
    )
    assert "14.4 * 0.01" in fast_burn["expr"]
    assert fast_burn["labels"]["slo"] == "orders-availability"


def test_reliability_dashboard_exposes_slo_consumer_and_alert_panels() -> None:
    dashboard = json.loads(
        (ROOT / "observability" / "grafana" / "traceharbor-dashboard.json").read_text()
    )
    titles = {panel["title"] for panel in dashboard["panels"]}

    assert dashboard["title"] == "TraceHarbor reliability"
    assert "Orders availability error ratio (SLO target < 1%)" in titles
    assert "Consumer terminal outcomes" in titles
    assert "Firing reliability alerts" in titles

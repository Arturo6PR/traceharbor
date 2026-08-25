import asyncio
import io
import json
from pathlib import Path

import httpx
import pytest
import yaml

from traceharbor.contracts import PaymentRequest, Scenario
from traceharbor.observability import (
    ObservabilityConfig,
    TelemetryMode,
    TelemetryRuntime,
)
from traceharbor.services.payments import create_payment_app
from traceharbor.tracecontext import TraceContext

ROOT = Path(__file__).resolve().parents[1]


async def _no_sleep(seconds: float) -> None:
    del seconds


async def _post_payment(app, scenario: Scenario, traceparent: str) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://payments.test"
    ) as client:
        return await client.post(
            "/v1/charges",
            json=PaymentRequest(
                order_id="order-otel-1", amount_cents=12500, currency="USD"
            ).model_dump(mode="json"),
            headers={
                "traceparent": traceparent,
                "x-traceharbor-scenario": scenario.value,
            },
        )


def test_observability_configuration_from_environment() -> None:
    disabled = ObservabilityConfig.from_environment("orders", {})
    console = ObservabilityConfig.from_environment(
        "payments",
        {
            "TRACEHARBOR_TELEMETRY_MODE": "CONSOLE",
            "OTEL_METRIC_EXPORT_INTERVAL": "250",
        },
    )
    otlp = ObservabilityConfig.from_environment(
        "inventory",
        {
            "TRACEHARBOR_TELEMETRY_MODE": "otlp",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.test:4318/",
        },
    )

    assert disabled.mode is TelemetryMode.DISABLED
    assert console.mode is TelemetryMode.CONSOLE
    assert console.metric_export_interval_ms == 250
    assert otlp.otlp_endpoint == "http://collector.test:4318"


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({"TRACEHARBOR_TELEMETRY_MODE": "unknown"}, "must be one of"),
        (
            {
                "TRACEHARBOR_TELEMETRY_MODE": "otlp",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4318",
            },
            "absolute http",
        ),
        (
            {
                "TRACEHARBOR_TELEMETRY_MODE": "otlp",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318/v1/traces",
            },
            "base URL",
        ),
        ({"OTEL_METRIC_EXPORT_INTERVAL": "fast"}, "must be an integer"),
        ({"OTEL_METRIC_EXPORT_INTERVAL": "10"}, "between 100 and 60000"),
    ],
)
def test_invalid_observability_configuration_is_rejected(
    environment: dict[str, str], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        ObservabilityConfig.from_environment("orders", environment)


def test_console_telemetry_correlates_trace_metrics_and_logs() -> None:
    stream = io.StringIO()
    runtime = TelemetryRuntime(
        ObservabilityConfig(
            service_name="payments",
            mode=TelemetryMode.CONSOLE,
            metric_export_interval_ms=60_000,
        ),
        console_stream=stream,
    )
    app = create_payment_app(observability=runtime, sleep=_no_sleep)
    parent = TraceContext(trace_id="1" * 32, span_id="2" * 16)

    response = asyncio.run(_post_payment(app, Scenario.PAYMENT_FAILURE, parent.traceparent))
    assert response.status_code == 503
    body = response.json()
    assert body["trace_id"] == parent.trace_id
    assert body["span_id"] != parent.span_id
    assert body["parent_span_id"] == parent.span_id

    assert runtime.force_flush()
    runtime.shutdown()
    exported = stream.getvalue()
    assert '"service.name": "traceharbor.payments"' in exported
    assert '"trace_id": "0x11111111111111111111111111111111"' in exported
    assert "service_step" in exported
    assert "traceharbor.service.steps" in exported
    assert "payment_failure" in exported


def test_disabled_telemetry_has_no_export_side_effects() -> None:
    runtime = TelemetryRuntime.disabled("orders")

    assert not runtime.enabled
    assert runtime.force_flush()
    runtime.shutdown()
    runtime.shutdown()


def test_local_observability_compose_is_pinned_and_loopback_only() -> None:
    compose = yaml.safe_load((ROOT / "compose.observability.yaml").read_text(encoding="utf-8"))
    services = compose["services"]

    assert set(services) == {"grafana", "loki", "otel-collector", "prometheus", "tempo"}
    for service in services.values():
        image = service["image"]
        assert ":" in image
        assert not image.endswith(":latest")
        assert "no-new-privileges:true" in service["security_opt"]
        for port in service.get("ports", []):
            assert port.startswith("127.0.0.1:")


def test_collector_routes_each_signal_to_the_expected_local_backend() -> None:
    collector = yaml.safe_load(
        (ROOT / "observability" / "otel-collector.yaml").read_text(encoding="utf-8")
    )
    pipelines = collector["service"]["pipelines"]

    assert pipelines["traces"]["exporters"] == ["otlphttp/tempo", "debug"]
    assert pipelines["metrics"]["exporters"] == ["prometheus", "debug"]
    assert pipelines["logs"]["exporters"] == ["otlphttp/loki", "debug"]
    assert all(pipeline["receivers"] == ["otlp"] for pipeline in pipelines.values())
    assert all(
        pipeline["processors"] == ["memory_limiter", "batch"] for pipeline in pipelines.values()
    )


def test_grafana_assets_are_valid_and_reference_provisioned_sources() -> None:
    grafana = ROOT / "observability" / "grafana"
    sources = yaml.safe_load((grafana / "datasources.yaml").read_text(encoding="utf-8"))
    dashboard = json.loads((grafana / "traceharbor-dashboard.json").read_text(encoding="utf-8"))

    assert {source["uid"] for source in sources["datasources"]} == {
        "loki",
        "prometheus",
        "tempo",
    }
    assert dashboard["uid"] == "traceharbor-overview"
    assert {panel["datasource"]["uid"] for panel in dashboard["panels"]} == {"prometheus"}

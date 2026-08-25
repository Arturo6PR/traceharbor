# TraceHarbor

TraceHarbor is a local distributed-systems observability laboratory. It makes healthy, slow, and
failed checkout requests reproducible across Orders, Payments, and Inventory, then connects the
evidence with W3C trace context and OpenTelemetry.

The project answers a practical platform-engineering question:

> When a transaction becomes slow or fails, can we reproduce it, follow the request across every
> service, and correlate its traces, metrics, and logs without relying on a cloud account?

## Current capabilities - Phase 2

- Three FastAPI services with explicit Orders-to-Payments/Inventory gateway boundaries.
- W3C `traceparent` validation, creation, and downstream propagation.
- Reproducible `healthy`, `payment_latency`, `payment_failure`, and `inventory_failure` scenarios.
- OpenTelemetry server spans plus service-step attributes, status, metrics, and correlated logs.
- `disabled`, `console`, and OTLP/HTTP telemetry modes selected through validated configuration.
- A local Collector routing traces to Tempo, metrics to Prometheus, and logs to Loki.
- Provisioned Grafana data sources and a small service-health dashboard.
- A deterministic in-process demo with stable report-schema `1.0` JSON and explicit exit codes.
- Cross-platform Python CI and behavior-focused pytest/Ruff validation.

The default telemetry mode is `disabled`, which keeps the deterministic demo byte-repeatable and
lets the services run without Docker. The observability stack is opt-in and local-only. TraceHarbor
does **not** claim Kafka/Redpanda, Kubernetes, Helm, cloud deployment, production authentication,
database persistence, or production-grade payment/inventory behavior yet.

## Architecture

```text
client
  |
  v
Orders --------> Payments
  |
  +------------> Inventory
  |
  +--- W3C trace context across every HTTP boundary
  |
  +--- OpenTelemetry SDK (traces + metrics + correlated logs)
                         |
                         v
                OpenTelemetry Collector
                  |        |        |
                  v        v        v
                Tempo  Prometheus  Loki
                  \        |        /
                   \       |       /
                         Grafana
```

The deterministic demo uses HTTPX ASGI transports, so requests still cross the same FastAPI
boundaries without opening network ports. Live development uses ordinary local HTTP clients. SDK
setup and exporters remain isolated in `observability.py`; service behavior depends only on an
injected telemetry runtime.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the component and trust boundaries.

## Install

TraceHarbor requires Python 3.11 or newer.

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
```

macOS or Linux:

```bash
python -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
```

## Sixty-second deterministic demo

No servers or containers are needed:

```shell
traceharbor demo --scenario healthy
traceharbor demo --scenario payment_latency
traceharbor demo --scenario payment_failure
traceharbor demo --scenario inventory_failure
```

Representative degraded output:

```text
TraceHarbor deterministic demo
Scenario: payment_latency
Outcome: DEGRADED
Trace ID: <stable 32-character trace ID>
Order: order-demo-001
Steps: ok=1, degraded=2, failed=0
- orders [DEGRADED] order completed with degraded dependency (...)
- payments [DEGRADED] payment authorized after simulated latency (...)
- inventory [OK] inventory reserved (...)
```

Request deterministic JSON or write it directly to a new file:

```shell
traceharbor demo --scenario healthy --format json
traceharbor demo --scenario payment_latency --format json --output reports/latency.json
```

`--output` leaves standard output empty and refuses to replace an existing file. Operational
diagnostics go to standard error. JSON keys, service ordering, simulated delays, and demo trace IDs
are stable for the same seed; no wall-clock timestamps or machine paths enter the report.

## Exit codes

| Exit code | Meaning |
|---:|---|
| `0` | The scenario completed healthy. |
| `10` | The scenario completed with a degraded dependency. |
| `20` | The transaction failed because a dependency failed. |
| `2` | An input, output, configuration, or operational error prevented completion. |

## Run with OpenTelemetry locally

Docker is needed only for the observability backends. Start the pinned local stack:

```powershell
docker compose -f compose.observability.yaml up -d
```

In each of three PowerShell terminals, set OTLP mode before starting one service:

```powershell
$env:TRACEHARBOR_TELEMETRY_MODE = "otlp"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://127.0.0.1:4318"
traceharbor serve payments
```

Repeat for `inventory` and `orders`. Their default ports are Payments `8002`, Inventory `8003`, and
Orders `8001`. Orders reads `TRACEHARBOR_PAYMENT_URL` and `TRACEHARBOR_INVENTORY_URL` when custom
local addresses are needed.

Send a request to Orders:

```powershell
$body = @{
  order_id = "order-local-001"
  item_id = "signal-adapter"
  amount_cents = 12500
  currency = "USD"
  quantity = 1
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8001/v1/orders" `
  -ContentType "application/json" `
  -Headers @{ "x-traceharbor-scenario" = "payment_latency" } `
  -Body $body
```

Open Grafana at <http://127.0.0.1:3000>. Prometheus, Tempo, and Loki are already provisioned. All
published ports bind to `127.0.0.1`; this configuration is a development lab, not a public service.

To inspect telemetry without Docker, use `console` mode instead:

```powershell
$env:TRACEHARBOR_TELEMETRY_MODE = "console"
traceharbor serve payments
```

Supported configuration:

| Variable | Default | Purpose |
|---|---|---|
| `TRACEHARBOR_TELEMETRY_MODE` | `disabled` | `disabled`, `console`, or `otlp`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://127.0.0.1:4318` | OTLP/HTTP base URL; signal paths are added by TraceHarbor. |
| `OTEL_METRIC_EXPORT_INTERVAL` | `5000` | Metric export interval in milliseconds, from 100 through 60000. |

Stop the local stack with `docker compose -f compose.observability.yaml down`. See
[`observability/README.md`](observability/README.md) for its data-retention note.

## Report contract

The deterministic demo report schema remains `1.0` and is documented in
[`docs/report-schema-v1.0.json`](docs/report-schema-v1.0.json). It contains the scenario, overall
outcome, one shared trace ID, ordered service steps, parent/child span identifiers, simulated
latency, and status counts. The versioned report is separate from live OpenTelemetry payloads.

## Develop and verify

```shell
ruff check .
ruff format --check .
pytest
docker compose -f compose.observability.yaml config --quiet
```

Tests cover strict contracts, trace parsing and lineage, all scenario outcomes, correlated
OpenTelemetry console exports, configuration errors, pinned/loopback-only Compose services,
Collector signal routing, Grafana provisioning, deterministic JSON, output-file safety,
stdout/stderr separation, schema validation, rendering, and every CLI exit code.

## Roadmap

1. **Phase 1 - service foundations:** completed deterministic topology, fault profiles, report, and
   trace-context boundaries.
2. **Phase 2 - standard observability:** completed local traces, metrics, logs, Collector routing,
   Prometheus, Tempo, Loki, and Grafana configuration.
3. **Phase 3 - asynchronous work:** Kafka-compatible Redpanda events, idempotent consumers, retries,
   and a dead-letter queue.
4. **Phase 4 - local platform:** containerized services, then `kind`, Helm, probes, resource limits,
   rolling updates, and local failure exercises.
5. **Phase 5 - reliability:** SLOs, error budgets, alerts, runbooks, load testing, and recovery
   verification.

Cloud deployment would be considered only after the local platform is useful, tested, and
cost-bounded. Phase 2 creates no AWS or other cloud resources.

## License

MIT

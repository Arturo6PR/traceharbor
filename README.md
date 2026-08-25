# TraceHarbor

TraceHarbor is a local event-driven distributed-systems laboratory. It makes healthy, slow, and
failed checkout requests reproducible across Orders, Payments, and Inventory, connects the evidence
with OpenTelemetry, and carries each outcome into a Redpanda-compatible event pipeline.

The project answers a practical platform-engineering question:

> When a transaction becomes slow or fails, can we reproduce it, follow the request across every
> service, correlate its telemetry, and safely process the resulting asynchronous event without
> relying on a cloud account?

## Current capabilities - Phase 5

- Three FastAPI services with explicit Orders-to-Payments/Inventory gateway boundaries.
- W3C `traceparent` validation, creation, and downstream propagation.
- Reproducible `healthy`, `payment_latency`, `payment_failure`, and `inventory_failure` scenarios.
- OpenTelemetry server spans plus service-step attributes, status, metrics, and correlated logs.
- `disabled`, `console`, and OTLP/HTTP telemetry modes selected through validated configuration.
- A local Collector routing traces to Tempo, metrics to Prometheus, and logs to Loki.
- Provisioned Grafana data sources and a small service-health dashboard.
- Versioned `order.outcome.recorded` events keyed by order ID with propagated trace context.
- OpenTelemetry consumer spans and correlated processing metrics/logs for valid events.
- A pinned single-broker Redpanda and Redpanda Console development topology.
- Idempotent Kafka production, manual consumer offset commits, and persistent SQLite deduplication.
- Bounded exponential retries and a versioned dead-letter record for malformed or exhausted work.
- One multi-stage, non-root application image shared by every service and the consumer.
- A unified local Compose path with read-only application filesystems, health checks, and loopback
  ports.
- A validated Helm chart with rolling updates, probes, resource budgets, and hardened pod settings.
- A disposable `kind` deployment and live cross-service smoke test in CI.
- A 99% request-based Orders availability SLO with multi-window error-budget burn alerts.
- Consumer DLQ-ratio and telemetry-continuity alerts validated with Prometheus rule tests.
- A bounded concurrent live-load gate with versioned JSON, explicit thresholds, and exit code `30`.
- A deterministic consumer restart/deduplication recovery verification and versioned report.
- A reliability dashboard, incident runbook, and reproducible local failure exercises.
- A deterministic in-process demo with stable report-schema `1.0` JSON and explicit exit codes.
- Cross-platform Python CI and behavior-focused pytest/Ruff validation.

Telemetry and event publishing are both disabled by default, keeping the deterministic demo
byte-repeatable and usable without Docker. The Redpanda and observability stacks are opt-in and
local-only. TraceHarbor does **not** claim exactly-once processing, a transactional outbox, a
production Kubernetes deployment, cloud deployment, public ingress, TLS, production
authentication, or production-grade payment/inventory behavior.

## Architecture

```text
client
  |
  v
Orders --------> Payments
  |
  +------------> Inventory
  |
  +------------> Redpanda ----> order-audit consumer
                                  |       |
                                  |       +----> bounded retries
                                  +------------> SQLite deduplication / DLQ
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

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the component boundaries and
[`docs/EVENTING.md`](docs/EVENTING.md) for delivery semantics and known limitations. Container,
Compose, Helm, and `kind` instructions are in [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

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
| `30` | A live-load or recovery reliability gate failed its declared threshold. |
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

## Run the complete containerized lab

Use the three layered Compose files to start the services, consumer, broker, and observability
backends on one private network:

```powershell
docker compose `
  -f compose.observability.yaml `
  -f compose.events.yaml `
  -f compose.apps.yaml `
  up --build -d
```

Orders is available at <http://127.0.0.1:8001>, Redpanda Console at
<http://127.0.0.1:8080>, and Grafana at <http://127.0.0.1:3000>. Application containers run as a
dedicated non-root user with read-only root filesystems; only the consumer's local SQLite ledger
receives a writable named volume. See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) for lifecycle,
Helm, and `kind` instructions.

## Run the event pipeline locally

Start the pinned single-broker Redpanda topology:

```powershell
docker compose -f compose.events.yaml up -d
```

The topology creates `traceharbor.orders.v1` with three partitions and
`traceharbor.orders.dlq.v1` with one partition. Redpanda Console is available at
<http://127.0.0.1:8080>.

Start Payments and Inventory normally. In the Orders terminal, enable Kafka-compatible event
publishing before starting the service:

```powershell
$env:TRACEHARBOR_EVENTS_MODE = "kafka"
$env:TRACEHARBOR_KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:19092"
traceharbor serve orders
```

Start the audit consumer in another terminal:

```powershell
$env:TRACEHARBOR_KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:19092"
traceharbor consume order-audit
```

Submitting an order publishes one compact event for its `HEALTHY`, `DEGRADED`, or `FAILED`
outcome. The consumer validates the payload and required headers, skips already completed event
IDs, retries handler failures up to three times, and sends invalid or exhausted records to the DLQ.
Offsets are committed synchronously only after processing, duplicate recognition, or successful DLQ
publication.

Use `TRACEHARBOR_EVENTS_MODE=console` to inspect the exact event without a broker. Consumer state is
stored in `.traceharbor/processed-events.sqlite3` by default. The relevant settings are documented
in [`docs/EVENTING.md`](docs/EVENTING.md).

## Versioned contracts

The deterministic demo report schema remains `1.0` and is documented in
[`docs/report-schema-v1.0.json`](docs/report-schema-v1.0.json). It contains the scenario, overall
outcome, one shared trace ID, ordered service steps, parent/child span identifiers, simulated
latency, and status counts. The versioned report is separate from live OpenTelemetry payloads.

The asynchronous contracts are checked in as
[`docs/order-event-schema-v1.0.json`](docs/order-event-schema-v1.0.json) and
[`docs/dead-letter-schema-v1.0.json`](docs/dead-letter-schema-v1.0.json). Payloads use canonical,
sorted JSON without machine paths or application-generated timestamps. Broker offsets provide the
transport ordering metadata.

The reliability commands use separate version `1.0` contracts:
[`docs/load-report-schema-v1.0.json`](docs/load-report-schema-v1.0.json) and
[`docs/recovery-report-schema-v1.0.json`](docs/recovery-report-schema-v1.0.json). Load reports
contain measured latency and are intentionally not deterministic; recovery reports are
byte-repeatable.

## Reliability verification

Run the restart-safe deduplication check without Docker or a broker:

```shell
traceharbor verify consumer-recovery --format json
```

With a live Orders service running, execute a bounded release gate:

```shell
traceharbor load --url http://127.0.0.1:8001 --requests 100 --concurrency 10 --max-error-rate 0.01 --max-p95-ms 500 --format json
```

Prometheus evaluates the 99% Orders availability SLO, fast/slow error-budget burn alerts, consumer
DLQ ratio, and Collector continuity. The Grafana dashboard exposes their source metrics and firing
state. See [`docs/SLO.md`](docs/SLO.md), [`docs/RUNBOOK.md`](docs/RUNBOOK.md), and
[`docs/FAILURE_EXERCISES.md`](docs/FAILURE_EXERCISES.md). No notification channel or external
incident system is configured.

## Develop and verify

```shell
ruff check .
ruff format --check .
pytest
docker compose -f compose.observability.yaml config --quiet
docker compose -f compose.events.yaml config --quiet
docker compose -f compose.observability.yaml -f compose.events.yaml -f compose.apps.yaml config --quiet
helm lint deploy/helm/traceharbor
helm template traceharbor deploy/helm/traceharbor
traceharbor verify consumer-recovery --format json
```

Tests cover strict contracts, trace parsing and lineage, all scenario outcomes, correlated
OpenTelemetry console exports, configuration errors, pinned/loopback-only Compose services,
Collector signal routing, Grafana provisioning, event identity and headers, producer delivery
failures, manual commit behavior, persistent deduplication, retry schedules, DLQ routing, both event
schemas, deterministic JSON, output-file safety, stdout/stderr separation, rendering, and every CLI
exit code. CI additionally builds and smoke-tests the image, renders the chart, creates a disposable
`kind` cluster, verifies all HTTP rollouts, and calls the live Orders topology.

## Roadmap

1. **Phase 1 - service foundations:** completed deterministic topology, fault profiles, report, and
   trace-context boundaries.
2. **Phase 2 - standard observability:** completed local traces, metrics, logs, Collector routing,
   Prometheus, Tempo, Loki, and Grafana configuration.
3. **Phase 3 - asynchronous work:** completed Redpanda-compatible events, idempotent producer and
   consumer boundaries, manual commits, persistent deduplication, retries, and a dead-letter queue.
4. **Phase 4 - local platform:** completed containerized services, unified Compose, `kind`, Helm,
   probes, resource limits, hardened pod settings, rolling updates, and live topology smoke tests.
5. **Phase 5 - reliability:** completed request-based SLOs, error budgets, tested alerts, runbooks,
   bounded load testing, failure exercises, and deterministic recovery verification.

Cloud deployment would be considered only after the local platform is useful, tested, and
cost-bounded. Phases 1-5 create no AWS or other cloud resources.

## License

MIT

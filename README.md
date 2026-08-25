# TraceHarbor

TraceHarbor is a local distributed-service laboratory for learning how a request moves through
multiple services, how failures propagate, and how one trace identifier connects the resulting
evidence. Phase 1 implements an Orders service that calls Payments and Inventory through explicit
gateway interfaces and propagates W3C `traceparent` headers across every boundary.

The project answers a practical platform-engineering question:

> When a transaction becomes slow or fails, can we reproduce the behavior and follow the same
> request across every participating service?

## Current capabilities - Phase 1

- Three FastAPI services: Orders, Payments, and Inventory.
- W3C trace-context parsing, validation, creation, and downstream propagation.
- Reproducible `healthy`, `payment_latency`, `payment_failure`, and `inventory_failure` scenarios.
- Strict Pydantic request, step, telemetry-event, and report contracts.
- Structured service events sharing the propagated trace ID.
- A deterministic in-process demonstration that needs no running servers.
- Stable text and report-schema `1.0` JSON output.
- Distinct exit codes for healthy, degraded, failed, and operational outcomes.
- Behavior-focused pytest coverage and Ruff configuration.

Phase 1 is intentionally local and small. It does **not** yet claim OpenTelemetry SDK/Collector,
Prometheus, Grafana, Kafka/Redpanda, Docker Compose, Kubernetes, Helm, cloud deployment, production
authentication, or production-grade payment/inventory behavior. Those are later phases, not hidden
dependencies of the current demo.

## Architecture

```text
traceharbor demo
       |
       v
Orders service
  |          |
  v          v
Payments   Inventory
  |          |
  +---- propagated W3C trace context ----+
                                          |
                                          v
                              versioned scenario report
```

The deterministic demonstration uses HTTPX ASGI transports, so requests still cross the same HTTP
application boundaries without opening network ports. The live development mode uses ordinary HTTP
clients and three local ports.

Application responsibilities remain separate:

```text
contracts -> trace context -> centralized fault profiles
                                  |
                                  v
service apps -> gateway interfaces -> scenario result -> renderer / CLI
      |
      v
structured event sink
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the trust boundaries and later-phase seams.

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

## Sixty-second demonstration

Run the four scenarios without starting any servers:

```shell
traceharbor demo --scenario healthy
traceharbor demo --scenario payment_latency
traceharbor demo --scenario payment_failure
traceharbor demo --scenario inventory_failure
```

Representative degraded output:

```text
TraceHarbor Phase 1
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

## Run the services on local ports

The deterministic demo is the fastest path. To observe real HTTP calls, open three terminals from
the repository's activated environment:

```powershell
traceharbor serve payments
traceharbor serve inventory
traceharbor serve orders
```

The defaults are Orders `8001`, Payments `8002`, and Inventory `8003`. The Orders service reads
`TRACEHARBOR_PAYMENT_URL` and `TRACEHARBOR_INVENTORY_URL` when custom downstream locations are
needed. The other service commands accept `--host` and `--port`.

Create a healthy order from PowerShell:

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
  -Headers @{ "x-traceharbor-scenario" = "healthy" } `
  -Body $body
```

The development services emit one-line JSON service events. Phase 1 intentionally omits wall-clock
timestamps so repository demonstrations remain reproducible. OpenTelemetry will own real span
timing and export semantics in Phase 2.

## Report contract

The external report schema is `1.0` and is documented in
[`docs/report-schema-v1.0.json`](docs/report-schema-v1.0.json). It includes:

- scenario and overall outcome;
- one shared 32-character trace ID;
- deterministic service steps with span and parent-span identifiers;
- explicit `OK`, `DEGRADED`, or `FAILED` status;
- simulated latency rather than nondeterministic wall-clock measurements; and
- status counts suitable for scripts and CI.

The telemetry-event contract is independently labeled `1.0`. It is a Phase 1 structured-log seam,
not a substitute for the OpenTelemetry protocol.

## Develop and verify

```shell
ruff check .
ruff format --check .
pytest
```

Tests cover strict validation, trace-context parsing, deterministic ID generation, healthy and
failure behavior, parent/child relationships, service health endpoints, structured events, report
schema validation, rendering, output-file safety, standard output/error separation, deterministic
JSON, and all exit codes.

## Roadmap

1. **Phase 2 - standard observability:** OpenTelemetry SDK instrumentation and Collector export for
   traces, metrics, and logs; Prometheus, Tempo/Loki, and Grafana locally.
2. **Phase 3 - asynchronous work:** Kafka-compatible Redpanda events, idempotent consumers, retries,
   and a dead-letter queue.
3. **Phase 4 - local platform:** Docker Compose first, then `kind`, Helm, probes, resource limits,
   rolling updates, and local failure exercises.
4. **Phase 5 - reliability:** SLOs, error budgets, alerts, runbooks, load testing, and recovery
   verification.

AWS or another cloud provider would only be considered after the entire local platform is useful,
tested, and cost-bounded.

## License

MIT

# Architecture

## Phase 2 objective

TraceHarbor separates deterministic service behavior from live observability. A checkout enters
Orders and crosses explicit Payment and Inventory gateways. W3C trace context preserves lineage;
an injected OpenTelemetry runtime records live spans, metrics, and correlated logs without changing
the versioned demo report.

## Application boundaries

### Contracts

Strict Pydantic models reject unknown properties and malformed identifiers at the HTTP boundary.
Scenario reports use schema version `1.0`; the original deterministic event seam has an independent
`1.0` contract. Neither contract is an OpenTelemetry wire format.

### Trace context

`tracecontext.py` owns validation and deterministic fallback IDs. With instrumentation enabled, the
active OpenTelemetry server span supplies the service trace/span ID. The incoming W3C span remains
the recorded parent, and gateways propagate the active service context downstream. With telemetry
disabled, the injected ID factory preserves the original byte-repeatable demo.

Only W3C version `00` is accepted by the explicit contract. All-zero IDs are rejected. Baggage,
tracestate, and remote sampling policy remain outside the current scope.

### Fault profiles

`scenarios.py` is the sole source of simulated status, detail, HTTP status, and delay. Handlers do
not carry separate fault rules. Demo latency is declared but not slept; the live Payment service
sleeps for the declared delay.

### Services and gateways

Payments and Inventory translate requests into service steps. Orders coordinates downstream calls,
stops before Inventory when Payment fails, derives the outcome, and emits the report. Live gateways
use HTTPX; the demo uses HTTPX ASGI transports against the same FastAPI apps.

## Observability boundaries

`observability.py` owns SDK resources, providers, instruments, processors, and exporters. Each
service receives its own runtime and resource identity (`traceharbor.orders`,
`traceharbor.payments`, or `traceharbor.inventory`); process-global providers are not mutated.

- FastAPI instrumentation creates server spans and extracts incoming W3C context.
- A counter records completed service steps by service, scenario, and status.
- A histogram records scenario-declared delay in milliseconds.
- Service-step logs use the OpenTelemetry log API inside the active span context.
- Failed work marks its server span as error; degraded work adds a span event.

`disabled` mode creates no SDK provider. `console` mode exports locally to standard output. `otlp`
mode uses OTLP/HTTP signal endpoints derived from one validated base URL.

```text
service SDK --OTLP--> Collector --traces--> Tempo
                            |------metrics--> Prometheus
                            |---------logs--> Loki

                 Grafana queries all three backends
```

The Collector applies a memory limiter before batching. Its debug exporter remains enabled for
local troubleshooting. Compose pins every image and binds published ports to loopback.

## Trust and failure boundaries

- Incoming JSON, scenario headers, and explicit trace context are untrusted and validated.
- Telemetry mode, export interval, service label, and OTLP base URL are validated at startup.
- Payments and Inventory are downstream dependencies; expected failures become structured steps.
- Deterministic reports exclude wall-clock time, paths, random demo IDs, and platform exceptions.
- Output files use exclusive creation and are never silently replaced.
- The lab stores no payment credentials, tokens, customer records, or cloud secrets.
- Grafana anonymous Viewer access is safe only because every published port is loopback-only.

## Deferred decisions

There is no database, broker, Kafka/Redpanda stream, Kubernetes object, Helm chart, public ingress,
TLS termination, production authentication, or cloud resource. The Compose file runs observability
backends only; application containers begin in Phase 4. These are explicit scope boundaries rather
than production-readiness claims.

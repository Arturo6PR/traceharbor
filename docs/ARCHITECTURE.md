# Architecture

## Phase 1 objective

Phase 1 establishes service and telemetry boundaries before adding an observability stack. A single
checkout request enters Orders, then crosses explicit Payment and Inventory gateway interfaces. The
same W3C trace ID is retained, while each service creates its own span ID and records the caller's
span ID as its parent.

## Components

### Contracts

Strict Pydantic models reject unknown properties and malformed identifiers at the HTTP boundary.
Scenario reports use schema version `1.0`; structured service events use their own `1.0` label.

### Trace context

`tracecontext.py` owns W3C `traceparent` parsing and generation. Live services use cryptographically
random identifiers. The checked-in demo injects a content-derived factory so repeated commands are
byte-identical and testable.

The Phase 1 implementation supports W3C version `00` only. It validates formatting and rejects
all-zero trace or span IDs. It does not attempt baggage, tracestate, remote sampling policy, clocks,
or exporter behavior; those belong to the OpenTelemetry phase.

### Fault profiles

`scenarios.py` is the only source of simulated service status, detail, HTTP status, and delay. The
service handlers do not embed separate fault rules. Demo latency is recorded but not slept, while
the live Payment service sleeps for the declared delay.

### Service applications

Payments and Inventory translate requests into service steps. Orders coordinates downstream calls,
stops before Inventory when Payment fails, derives the overall outcome, and emits the versioned
report. Expected dependency failures remain structured results; malformed inputs remain ordinary
HTTP client errors.

### Gateways

Orders depends on protocols rather than concrete transports. Live gateways use HTTPX over local
HTTP. The demo uses HTTPX ASGI transports against the same FastAPI applications. This keeps tests
fast without replacing the application boundary with direct function calls.

### Telemetry seam

Services emit strict structured events through an injected sink. The live development apps use
newline-delimited JSON; tests and demos inject collecting or null sinks. OpenTelemetry SDK code is
not present yet, so Phase 2 can replace the sink without mixing exporter logic into business flow.

## Trust and failure boundaries

- Incoming JSON, scenario headers, and trace context are untrusted and validated.
- Payments and Inventory are downstream dependencies; connection and response failures become
  failed steps rather than unhandled exceptions.
- Reports exclude timestamps, local paths, random IDs in demo mode, and exception messages that can
  vary by platform.
- Output files use exclusive creation and are never silently replaced.
- No service stores payment data, credentials, tokens, or customer records.

## Deferred decisions

Phase 1 has no database, broker, container, Kubernetes object, cloud resource, authentication,
secret, TLS termination, or OpenTelemetry Collector. Those omissions are explicit scope boundaries,
not production-readiness claims.

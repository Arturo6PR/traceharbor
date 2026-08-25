# Architecture

## Phase 4 objective

TraceHarbor separates deterministic service behavior, live telemetry, and asynchronous delivery. A
checkout enters Orders and crosses explicit Payment and Inventory gateways. Orders derives one
versioned result, returns it over HTTP, and can publish the same outcome through a
Redpanda-compatible Kafka boundary.

Phase 4 packages those boundaries without merging them. One immutable image exposes the existing
CLI, while Compose and Helm select each process through arguments and environment configuration.

## Application boundaries

### HTTP and report contracts

Strict Pydantic models reject unknown properties and malformed identifiers at the HTTP boundary.
Scenario reports remain at schema version `1.0`; adding event delivery does not change their stable
JSON or CLI exit codes.

### Trace context

`tracecontext.py` owns explicit W3C validation and deterministic fallback IDs. With instrumentation
enabled, the active OpenTelemetry server span supplies the service trace/span ID. HTTP gateways
propagate that context, and the Orders publisher places the resulting `traceparent` on the broker
record. The consumer rejects a trace header that does not match the event payload.

Only W3C version `00` is accepted by the explicit contract. All-zero IDs are rejected. Baggage,
tracestate, and remote sampling policy remain outside the current scope.

### Fault profiles, services, and gateways

`scenarios.py` is the sole source of simulated service status, HTTP status, detail, and delay.
Payments and Inventory translate requests into service steps. Orders coordinates downstream calls,
derives the outcome, emits the report, and delegates optional publication to an injected event
publisher. The deterministic demo injects the null publisher unless a test explicitly collects
events.

## Eventing boundaries

`events.py` owns event configuration, canonical serialization, publisher protocols, topic names,
and the `order.outcome.recorded` schema. `kafka.py` contains only Confluent Kafka transport adapters.
`consumer.py` owns validation, processing policy, persistent deduplication, retry scheduling, and
dead-letter construction.

```text
Orders
  |
  | PublishedEvent(topic, order key, canonical JSON, W3C headers)
  v
traceharbor.orders.v1
  |
  v
order-audit consumer
  |------ already processed -----> synchronous offset commit
  |------ handler succeeds ------> SQLite mark -> synchronous commit
  |------ invalid event ---------> traceharbor.orders.dlq.v1 -> commit
  +------ retries exhausted -----> traceharbor.orders.dlq.v1 -> commit
```

The producer enables Kafka idempotence and waits for delivery acknowledgement. The consumer
disables automatic commits and stores. It commits an explicit message offset only after a terminal
processing result. The SQLite ledger contains only completed event IDs and survives restarts.

This provides at-least-once consumption with application-level duplicate suppression. It is not an
exactly-once guarantee. See [`EVENTING.md`](EVENTING.md) for the crash window and dual-write limits.

## Observability boundaries

`observability.py` owns SDK resources, providers, instruments, processors, and exporters. Each HTTP
service receives its own runtime and resource identity. Process-global providers are not mutated.

- FastAPI instrumentation creates server spans and extracts incoming W3C context.
- A counter records completed service steps by service, scenario, and status.
- A histogram records scenario-declared delay in milliseconds.
- Service-step logs use the OpenTelemetry log API inside the active span context.
- Failed work marks its server span as error; degraded work adds a span event.
- Valid broker records continue the propagated trace as consumer spans and record terminal
  processing metrics/logs.

`disabled` mode creates no SDK provider. `console` mode exports locally. `otlp` mode sends OTLP/HTTP
to the Collector, which routes traces to Tempo, metrics to Prometheus, and logs to Loki. Grafana
queries all three local backends.

## Deployment boundaries

The multi-stage Dockerfile builds a wheel separately from the runtime. The final image runs as a
fixed non-root UID/GID and contains no source checkout, test suite, credentials, or environment
specific configuration. Compose supplies per-process commands, internal dependency addresses, a
read-only root filesystem, dropped capabilities, and the sole writable consumer-state volume.

The Helm chart deploys only the three HTTP applications and optional consumer. Redpanda and the
observability backends remain independently operated dependencies. HTTP Deployments use rolling
updates with zero planned unavailability, startup/readiness/liveness probes, explicit resource
requests and limits, no service-account token, and restricted pod/container security contexts.
Consumer state is an `emptyDir`, making the chart suitable for disposable `kind` validation but not
a claim of durable production storage.

## Trust and failure boundaries

- Incoming HTTP data, broker payloads, headers, configuration, and trace context are untrusted.
- Event identity is recomputed; outcome counts and payload/header trace IDs must agree.
- Malformed records do not reach the handler and require successful DLQ publication before commit.
- Handler exceptions use bounded retries; arbitrary exception text is excluded from the DLQ.
- Kafka delivery errors are surfaced instead of silently dropping events.
- Deterministic reports and events exclude wall-clock time, paths, and random demo IDs.
- The lab stores no payment credentials, tokens, customer records, or cloud secrets.
- Every Compose-published port binds to loopback; Kubernetes has no ingress by default.

## Deferred decisions

There is no transactional outbox, Schema Registry, multi-broker replication, persistent Kubernetes
consumer volume, public ingress, TLS termination, production authentication, autoscaling, or cloud
resource. The Compose and Helm definitions are local validation targets, not production-readiness
claims. Those omissions are explicit scope boundaries.

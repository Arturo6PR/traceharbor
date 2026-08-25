# Local failure exercises

These exercises are intentionally bounded to the local Compose lab. Run one at a time, preserve
the generated trace ID, and restore the healthy state before starting the next exercise.

## Dependency latency

Send `payment_latency` traffic. Expected evidence: HTTP remains successful, the overall outcome is
`DEGRADED`, Payments contains the declared delay, the shared trace spans all three services, and
the availability SLO does not count the transaction as failed.

## Payment failure

Send `payment_failure` traffic. Expected evidence: Payments returns a failure, Inventory is not
called, Orders returns dependency failure, one `FAILED` order event is produced, and the Orders
error ratio rises.

## Inventory failure

Send `inventory_failure` traffic. Expected evidence: Payment succeeds, Inventory fails, Orders
returns dependency failure, and one `FAILED` order event carries the same trace ID.

## Consumer restart

Run `traceharbor verify consumer-recovery --format json`. The command processes one fixed event,
closes the SQLite store, reopens it, and presents the same event again. Expected evidence:
`PROCESSED` before restart, `DUPLICATE` after restart, one handler invocation, zero DLQ events, and
exit code `0`.

## Invalid event and DLQ

The behavior-focused test suite submits malformed JSON, a tombstone, missing/duplicate headers, a
forged event ID, inconsistent outcome counts, and a trace mismatch. Expected evidence: the handler
is never called, a versioned sanitized DLQ record is published, and the source offset is eligible
for commit only after DLQ delivery succeeds.

## Telemetry interruption

Stop only `otel-collector`, then send one healthy request. Expected evidence: service health is
independent of the Collector, Prometheus eventually raises the telemetry alert, and telemetry
resumes after the Collector restarts. Do not describe missing telemetry as application success;
the exercise specifically demonstrates an observability blind spot.

## Load gate

Run the documented healthy load gate, then repeat with an intentionally impossible p95 threshold
such as `--max-p95-ms 1`. Expected evidence: the first run normally passes on an idle local machine;
the second returns exit `30` with `passed: false`. Measured latency is environmental and therefore
is not a deterministic fixture.

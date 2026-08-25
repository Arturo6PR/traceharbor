# Local incident runbook

The commands below target the complete local Compose lab. Preserve evidence before restarting or
deleting anything. Do not use `down --volumes` during diagnosis because it removes broker,
telemetry, Grafana, and consumer state.

## Orders availability burn

**Signal:** `TraceHarborOrdersAvailabilityFastBurn` or
`TraceHarborOrdersAvailabilitySlowBurn`.

1. Open the Grafana TraceHarbor dashboard and identify the failing scenario and dependency.
2. In Tempo, follow a failed Orders trace into Payments or Inventory. Match its trace ID in Loki.
3. Inspect recent service logs without changing state:
   `docker compose -f compose.observability.yaml -f compose.events.yaml -f compose.apps.yaml logs --since 15m orders payments inventory`.
4. If one dependency is unhealthy, stop new test traffic and restore that dependency. Do not mask
   failures by weakening the SLO or changing scenario labels.
5. Run a healthy request, then `traceharbor load` with the documented release thresholds.
6. Confirm both short and long error-ratio recordings are falling. Record the trace ID and cause.

## Dead-letter ratio

**Signal:** `TraceHarborConsumerDeadLetterRatioHigh`.

1. Open Redpanda Console and inspect `traceharbor.orders.dlq.v1` without replaying records.
2. Compare `reason`, `error_type`, schema header, payload schema version, and propagated trace ID.
   The DLQ intentionally excludes arbitrary exception text.
3. For `INVALID_EVENT`, correct the producer/contract mismatch before replay. For
   `HANDLER_RETRIES_EXHAUSTED`, correct the handler dependency before replay.
4. Preserve the original topic/partition/offset and dead-letter ID in incident notes.
5. TraceHarbor has no automated DLQ replay command; manual replay is intentionally deferred until
   an idempotent, audited policy exists.
6. Run `traceharbor verify consumer-recovery --format json` and confirm `passed: true` before
   restarting the consumer.

## Telemetry Collector unavailable

**Signal:** `TraceHarborTelemetryCollectorUnavailable`.

1. Treat this as loss of visibility, not proof that application traffic failed.
2. Inspect Collector, Prometheus, Tempo, and Loki health/logs. Check memory-limiter messages and the
   three OTLP pipelines.
3. Keep the services running if customer-path checks remain healthy; telemetry export is bounded
   and must not silently redefine request outcomes.
4. Restore the Collector, submit one known healthy order, and verify its trace, metric, and log all
   share the trace context.

## Consumer restart and deduplication

1. Run `traceharbor verify consumer-recovery --format json` before manual experimentation.
2. In Compose, stop only `order-consumer`; do not remove `consumer-data`.
3. Start it again and confirm an already completed event is reported as `DUPLICATE`, not handled a
   second time or sent to the DLQ.
4. If the SQLite volume was lost, duplicate suppression history is lost. This is a known local
   architecture limitation; stop processing until the impact is understood.

## Recovery completion criteria

- A healthy end-to-end Orders request succeeds.
- The live load gate passes its declared error-rate and p95 thresholds.
- The deterministic consumer recovery check passes.
- New traces, metrics, and logs arrive and correlate.
- Error-ratio and DLQ-ratio recordings trend back below their thresholds.
- No diagnostic volume was deleted before evidence was captured.

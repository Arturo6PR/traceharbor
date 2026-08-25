# Event delivery model

Phase 3 adds a deliberately small Kafka-compatible boundary without changing the deterministic
HTTP demo. The live Orders service publishes to Redpanda only when `TRACEHARBOR_EVENTS_MODE=kafka`.

## Topics and contracts

| Topic | Partitions | Key | Payload |
|---|---:|---|---|
| `traceharbor.orders.v1` | 3 | order ID | `order.outcome.recorded` schema `1.0` |
| `traceharbor.orders.dlq.v1` | 1 | event or dead-letter ID | dead-letter schema `1.0` |

The source topic uses the order ID as its Kafka key, keeping events for one order on the same
partition. Event IDs are deterministic hashes of event type, order ID, and trace ID. Re-delivery of
the same Kafka record therefore retains the same idempotency identity.

Every source message must contain exactly one each of these headers:

- `content-type: application/json`
- `traceharbor-event-schema: 1.0`
- a valid W3C `traceparent` whose trace ID matches the payload

Invalid JSON, unknown fields, forged event IDs, inconsistent outcome counts, missing or duplicated
headers, and trace mismatches bypass the handler and go directly to the DLQ.

## Producer behavior

The Confluent Kafka producer enables idempotence and `acks=all`, publishes a compact sorted JSON
payload, and waits for delivery confirmation. A timeout or broker rejection is surfaced to the
Orders request instead of being silently ignored.

This is not a transactional outbox: payment/inventory work and Kafka publication are not one atomic
transaction. A broker failure after downstream work can fail the HTTP request. Phase 3 keeps that
limitation visible rather than claiming dual-write safety it does not have.

## Consumer behavior

The `order-audit` consumer disables automatic commits and automatic offset storage. For each record:

1. Validate the versioned payload and required headers.
2. Skip the handler when the event ID is already in the SQLite processed-event ledger.
3. Run the handler up to `max_attempts` times using exponential backoff capped at ten seconds.
4. Mark the event processed only after the handler returns successfully.
5. Publish malformed or retry-exhausted work to the DLQ.
6. Commit that Kafka record synchronously only after a processed, duplicate, or dead-letter result.

With telemetry enabled, a valid event continues its W3C parent as an OpenTelemetry consumer span.
The terminal disposition and attempt count are recorded on that span, as a metric, and in a
correlated OpenTelemetry log.

If processing or DLQ publication raises, the offset is not committed and Redpanda can redeliver the
record. SQLite makes deduplication survive consumer restarts, but there is still a crash window
between a handler side effect and marking the event complete. Handlers with external side effects
must therefore be idempotent. TraceHarbor does not claim end-to-end exactly-once delivery.

## Configuration

| Variable | Default | Validation |
|---|---|---|
| `TRACEHARBOR_EVENTS_MODE` | `disabled` | `disabled`, `console`, or `kafka` |
| `TRACEHARBOR_KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:19092` | comma-separated `host:port` entries |
| `TRACEHARBOR_KAFKA_DELIVERY_TIMEOUT` | `5` | 0.1 through 60 seconds |
| `TRACEHARBOR_KAFKA_CONSUMER_GROUP` | `traceharbor-order-audit-v1` | nonempty label without whitespace |
| `TRACEHARBOR_CONSUMER_MAX_ATTEMPTS` | `3` | 1 through 10 |
| `TRACEHARBOR_CONSUMER_RETRY_BASE_DELAY_MS` | `100` | 0 through 10000 milliseconds |
| `TRACEHARBOR_CONSUMER_POLL_TIMEOUT` | `1` | 0.1 through 30 seconds |
| `TRACEHARBOR_CONSUMER_STATE_PATH` | `.traceharbor/processed-events.sqlite3` | local filesystem path |

`--max-messages N` is available on `traceharbor consume order-audit` for bounded exercises. Without
it, the consumer runs until interrupted.

## Local topology scope

`compose.events.yaml` pins one Redpanda development broker and Redpanda Console and binds host ports
to `127.0.0.1`. A single broker with replication factor one is intentionally not production-ready.
There is no cloud service, Schema Registry, authentication, TLS, or cross-region replication in
this phase.

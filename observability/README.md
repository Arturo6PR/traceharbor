# Local observability stack

This directory contains the local-only OpenTelemetry Collector, Prometheus, Tempo, Loki, and
Grafana configuration used by TraceHarbor Phase 2. The stack has no cloud dependency and binds its
host-facing ports to `127.0.0.1`.

Start it from the repository root:

```shell
docker compose -f compose.observability.yaml up -d
```

Set `TRACEHARBOR_TELEMETRY_MODE=otlp` before starting the three TraceHarbor services. The SDK sends
traces, metrics, and correlated logs to the Collector at `http://127.0.0.1:4318`. Grafana is then
available at <http://127.0.0.1:3000> with provisioned Prometheus, Tempo, and Loki data sources.

Stop the stack without deleting its named volumes:

```shell
docker compose -f compose.observability.yaml down
```

Add `--volumes` only when intentionally discarding all local observability data.

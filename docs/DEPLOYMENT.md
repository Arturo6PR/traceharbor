# Local platform deployment

TraceHarbor's application image is shared by Orders, Payments, Inventory, and the order-audit
consumer. The image uses a multi-stage build, runs as UID/GID `10001`, and contains only the
installed wheel and runtime dependencies. It has no shell entrypoint wrapper and no embedded
credentials.

## Complete Docker Compose lab

The application file is intentionally layered with the event and observability files so there is
one network and one project:

```powershell
docker compose `
  -f compose.observability.yaml `
  -f compose.events.yaml `
  -f compose.apps.yaml `
  up --build -d
```

This starts the three HTTP services, order-audit consumer, Redpanda, Redpanda Console,
OpenTelemetry Collector, Tempo, Prometheus, Loki, and Grafana. Application ports and UIs bind only
to loopback. Application containers have a read-only root filesystem, all Linux capabilities
dropped, `no-new-privileges`, and explicit health checks. The consumer alone receives a named
volume for its SQLite deduplication state.

Stop the lab without deleting data:

```powershell
docker compose -f compose.observability.yaml -f compose.events.yaml -f compose.apps.yaml down
```

Add `--volumes` only when you explicitly want to remove local broker, telemetry, Grafana, and
consumer state.

## Helm and kind

The chart deploys the application boundary; it does not install Redpanda or observability
backends. For a dependency-free application smoke test, disable telemetry, events, and the
consumer:

```powershell
kind create cluster --config deploy/kind-config.yaml
docker build -t traceharbor:local .
kind load docker-image traceharbor:local --name traceharbor
helm upgrade --install traceharbor deploy/helm/traceharbor `
  --set image.tag=local `
  --set telemetry.mode=disabled `
  --set events.mode=disabled `
  --set consumer.enabled=false `
  --wait
kubectl port-forward service/traceharbor-traceharbor-orders 8001:8001
```

For the complete eventing/telemetry topology, point `events.bootstrapServers` and
`telemetry.endpoint` at endpoints reachable from the cluster, then enable the consumer. The chart
uses rolling deployments, startup/readiness/liveness probes for HTTP services, resource requests
and limits, a read-only filesystem, non-root execution, no service-account token, and a temporary
consumer state volume. The chart deliberately does not claim persistent production storage,
ingress, TLS, authentication, autoscaling, or a production Kubernetes deployment.

CI builds and smoke-tests the image, lints and renders the chart, creates a disposable kind
cluster, deploys the dependency-free mode, waits for all three rollouts, and calls the live Orders
endpoint. No cloud account is involved.

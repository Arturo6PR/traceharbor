# Reliability objectives and release gate

TraceHarbor uses one user-facing service-level objective and two supporting operational signals.
These are local lab objectives backed by the checked-in metrics and rules; they are not claims
about a deployed production service.

## Orders availability SLO

- **Objective:** at least 99% of completed Orders transactions are not `FAILED` over a rolling
  30-day window.
- **Good event:** a completed Orders step with status `OK` or `DEGRADED`.
- **Total event:** every completed Orders step.
- **Error event:** a completed Orders step with status `FAILED`.
- **Error budget:** 1% of completed transactions in the window.

The metric is request-based. It does not reinterpret an idle minute as successful traffic. A
degraded transaction remains available but is visible separately through its status label.

The Collector explicitly uses Prometheus underscore escaping with type/unit suffixes, locking the
custom counter names used by the dashboard and alert rules instead of relying on exporter defaults.

Prometheus records error ratios over 5 minutes, 30 minutes, 1 hour, and 6 hours. Two multi-window
alerts spend the same 1% budget at different rates:

| Alert | Windows | Burn rate | Intent |
|---|---|---:|---|
| Fast burn | 5 minutes and 1 hour | `14.4x` | Detect a sharp, budget-threatening failure. |
| Slow burn | 30 minutes and 6 hours | `6x` | Detect a sustained regression without paging on a short spike. |

No alert is sent anywhere. Prometheus evaluates the local rules and Grafana displays their source
metrics; notification routing is deliberately out of scope.

## Supporting signals

- **Consumer dead-letter ratio:** warn when more than 5% of terminal event-processing results over
  5 minutes are `DEAD_LETTERED` for 10 minutes. This catches invalid contracts or exhausted
  handlers; it is not a user-facing SLO.
- **Telemetry continuity:** warn when Prometheus cannot scrape the Collector for 2 minutes. This
  detects loss of evidence rather than application unavailability.

## Live release-load gate

`traceharbor load` sends bounded concurrent requests to a live Orders service and evaluates error
rate and p95 latency thresholds:

```powershell
traceharbor load `
  --url http://127.0.0.1:8001 `
  --requests 100 `
  --concurrency 10 `
  --max-error-rate 0.01 `
  --max-p95-ms 500 `
  --format json
```

The gate exits `0` on pass, `30` when either threshold fails, and `2` for invalid configuration or
an operational failure. Transport errors and invalid successful-response contracts count as failed
requests rather than disappearing. Its
versioned report schema is [`load-report-schema-v1.0.json`](load-report-schema-v1.0.json).

This is a controlled release check, not a capacity benchmark. Client and server sharing one
machine, cold starts, Docker resource limits, and background processes all affect measured
latency. Unlike the deterministic demo and recovery report, a live load report is not expected to
be byte-identical because it contains measured durations.

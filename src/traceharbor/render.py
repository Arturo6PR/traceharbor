"""Stable human and machine renderers."""

import json

from traceharbor.contracts import ScenarioReport


def render_json(report: ScenarioReport) -> str:
    return (
        json.dumps(
            report.model_dump(mode="json"),
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )


def render_text(report: ScenarioReport) -> str:
    lines = [
        "TraceHarbor deterministic demo",
        f"Scenario: {report.scenario.value}",
        f"Outcome: {report.outcome.value}",
        f"Trace ID: {report.trace_id}",
        f"Order: {report.order_id}",
        (
            "Steps: "
            f"ok={report.counts.ok}, degraded={report.counts.degraded}, "
            f"failed={report.counts.failed}"
        ),
    ]
    for step in report.steps:
        delay = f", simulated_delay_ms={step.simulated_delay_ms}" if step.simulated_delay_ms else ""
        lines.append(
            f"- {step.service} [{step.status.value}] {step.detail} (span={step.span_id}{delay})"
        )
    return "\n".join(lines) + "\n"

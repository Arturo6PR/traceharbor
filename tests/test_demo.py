import asyncio

import pytest

from traceharbor.contracts import Outcome, Scenario, StepStatus
from traceharbor.demo import run_demo


@pytest.mark.parametrize(
    ("scenario", "outcome", "services", "statuses"),
    [
        (
            Scenario.HEALTHY,
            Outcome.HEALTHY,
            ["orders", "payments", "inventory"],
            [StepStatus.OK, StepStatus.OK, StepStatus.OK],
        ),
        (
            Scenario.PAYMENT_LATENCY,
            Outcome.DEGRADED,
            ["orders", "payments", "inventory"],
            [StepStatus.DEGRADED, StepStatus.DEGRADED, StepStatus.OK],
        ),
        (
            Scenario.PAYMENT_FAILURE,
            Outcome.FAILED,
            ["orders", "payments"],
            [StepStatus.FAILED, StepStatus.FAILED],
        ),
        (
            Scenario.INVENTORY_FAILURE,
            Outcome.FAILED,
            ["orders", "payments", "inventory"],
            [StepStatus.FAILED, StepStatus.OK, StepStatus.FAILED],
        ),
    ],
)
def test_scenario_behavior(
    scenario: Scenario,
    outcome: Outcome,
    services: list[str],
    statuses: list[StepStatus],
) -> None:
    report = asyncio.run(run_demo(scenario))

    assert report.outcome is outcome
    assert [step.service for step in report.steps] == services
    assert [step.status for step in report.steps] == statuses
    assert all(step.trace_id == report.trace_id for step in report.steps)
    assert all(step.parent_span_id == report.steps[0].span_id for step in report.steps[1:])
    assert report.counts.ok + report.counts.degraded + report.counts.failed == len(report.steps)


def test_latency_is_simulated_without_wall_clock_data() -> None:
    report = asyncio.run(run_demo(Scenario.PAYMENT_LATENCY))
    payment = next(step for step in report.steps if step.service == "payments")
    assert payment.simulated_delay_ms == 750
    assert "timestamp" not in report.model_dump(mode="json")


def test_repeated_demo_reports_are_equal() -> None:
    first = asyncio.run(run_demo(Scenario.HEALTHY, seed="same"))
    second = asyncio.run(run_demo(Scenario.HEALTHY, seed="same"))
    assert first == second

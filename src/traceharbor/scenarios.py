"""Centralized deterministic fault profiles."""

from dataclasses import dataclass

from traceharbor.contracts import Scenario, StepStatus


@dataclass(frozen=True, slots=True)
class ServiceBehavior:
    status: StepStatus
    detail: str
    status_code: int
    simulated_delay_ms: int = 0


@dataclass(frozen=True, slots=True)
class FaultProfile:
    payment: ServiceBehavior
    inventory: ServiceBehavior


PROFILES: dict[Scenario, FaultProfile] = {
    Scenario.HEALTHY: FaultProfile(
        payment=ServiceBehavior(StepStatus.OK, "payment authorized", 200),
        inventory=ServiceBehavior(StepStatus.OK, "inventory reserved", 200),
    ),
    Scenario.PAYMENT_LATENCY: FaultProfile(
        payment=ServiceBehavior(
            StepStatus.DEGRADED,
            "payment authorized after simulated latency",
            200,
            simulated_delay_ms=750,
        ),
        inventory=ServiceBehavior(StepStatus.OK, "inventory reserved", 200),
    ),
    Scenario.PAYMENT_FAILURE: FaultProfile(
        payment=ServiceBehavior(StepStatus.FAILED, "payment service unavailable", 503),
        inventory=ServiceBehavior(StepStatus.OK, "inventory reserved", 200),
    ),
    Scenario.INVENTORY_FAILURE: FaultProfile(
        payment=ServiceBehavior(StepStatus.OK, "payment authorized", 200),
        inventory=ServiceBehavior(StepStatus.FAILED, "inventory reservation rejected", 409),
    ),
}


def parse_scenario(value: str | None) -> Scenario:
    if value is None:
        return Scenario.HEALTHY
    try:
        return Scenario(value)
    except ValueError as exc:
        supported = ", ".join(scenario.value for scenario in Scenario)
        raise ValueError(f"unsupported scenario {value!r}; choose one of: {supported}") from exc

"""Strict request, service-step, and report contracts."""

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

REPORT_SCHEMA_VERSION = "1.0"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Scenario(StrEnum):
    HEALTHY = "healthy"
    PAYMENT_LATENCY = "payment_latency"
    PAYMENT_FAILURE = "payment_failure"
    INVENTORY_FAILURE = "inventory_failure"


class StepStatus(StrEnum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class Outcome(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class CheckoutRequest(StrictModel):
    order_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9._-]+$")
    item_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9._-]+$")
    amount_cents: int = Field(gt=0, le=10_000_000)
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
    quantity: int = Field(default=1, ge=1, le=100)


class PaymentRequest(StrictModel):
    order_id: str = Field(min_length=1, max_length=64)
    amount_cents: int = Field(gt=0, le=10_000_000)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class InventoryRequest(StrictModel):
    order_id: str = Field(min_length=1, max_length=64)
    item_id: str = Field(min_length=1, max_length=64)
    quantity: int = Field(ge=1, le=100)


class ServiceStep(StrictModel):
    service: Literal["orders", "payments", "inventory"]
    status: StepStatus
    detail: str = Field(min_length=1, max_length=200)
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    span_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    parent_span_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    simulated_delay_ms: int = Field(default=0, ge=0, le=60_000)


class OutcomeCounts(StrictModel):
    ok: int = Field(ge=0)
    degraded: int = Field(ge=0)
    failed: int = Field(ge=0)


class ScenarioReport(StrictModel):
    report_schema_version: Literal["1.0"] = REPORT_SCHEMA_VERSION
    scenario: Scenario
    outcome: Outcome
    order_id: str
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    counts: OutcomeCounts
    steps: tuple[ServiceStep, ...]


class TelemetryEvent(StrictModel):
    telemetry_schema_version: Literal["1.0"] = "1.0"
    event: Literal["service_step"] = "service_step"
    service: Literal["orders", "payments", "inventory"]
    scenario: Scenario
    status: StepStatus
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    span_id: str = Field(pattern=r"^[0-9a-f]{16}$")

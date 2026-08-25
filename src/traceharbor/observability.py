"""OpenTelemetry runtime configuration kept outside service behavior."""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import IO
from urllib.parse import urlparse

from fastapi import FastAPI
from opentelemetry import propagate, trace
from opentelemetry._logs import Logger, SeverityNumber
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    BatchLogRecordProcessor,
    ConsoleLogRecordExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.trace import SpanKind, Status, StatusCode

from traceharbor import __version__
from traceharbor.contracts import Scenario, ServiceStep, StepStatus


class TelemetryMode(StrEnum):
    DISABLED = "disabled"
    CONSOLE = "console"
    OTLP = "otlp"


@dataclass(frozen=True, slots=True)
class ObservabilityConfig:
    service_name: str
    mode: TelemetryMode = TelemetryMode.DISABLED
    otlp_endpoint: str = "http://127.0.0.1:4318"
    metric_export_interval_ms: int = 5_000

    def __post_init__(self) -> None:
        if re.fullmatch(r"[a-z][a-z0-9_-]*", self.service_name) is None:
            raise ValueError("service_name must be a lowercase service label")
        if not 100 <= self.metric_export_interval_ms <= 60_000:
            raise ValueError("metric export interval must be between 100 and 60000 milliseconds")
        parsed = urlparse(self.otlp_endpoint)
        if self.mode is TelemetryMode.OTLP and (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.path not in {"", "/"}
            or parsed.params
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("OTLP endpoint must be an absolute http:// or https:// base URL")

    @classmethod
    def from_environment(
        cls, service_name: str, environ: Mapping[str, str] | None = None
    ) -> ObservabilityConfig:
        values = os.environ if environ is None else environ
        raw_mode = values.get("TRACEHARBOR_TELEMETRY_MODE", TelemetryMode.DISABLED.value)
        try:
            mode = TelemetryMode(raw_mode.lower())
        except ValueError as exc:
            supported = ", ".join(mode.value for mode in TelemetryMode)
            raise ValueError(f"TRACEHARBOR_TELEMETRY_MODE must be one of: {supported}") from exc
        raw_interval = values.get("OTEL_METRIC_EXPORT_INTERVAL", "5000")
        try:
            interval = int(raw_interval)
        except ValueError as exc:
            raise ValueError("OTEL_METRIC_EXPORT_INTERVAL must be an integer") from exc
        return cls(
            service_name=service_name,
            mode=mode,
            otlp_endpoint=values.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318").rstrip(
                "/"
            ),
            metric_export_interval_ms=interval,
        )


class TelemetryRuntime:
    """Owns per-service SDK providers without mutating process-global providers."""

    def __init__(
        self,
        config: ObservabilityConfig,
        *,
        console_stream: IO[str] | None = None,
    ) -> None:
        self.config = config
        self._shutdown = False
        self.tracer_provider: TracerProvider | None = None
        self.meter_provider: MeterProvider | None = None
        self.logger_provider: LoggerProvider | None = None
        self._step_counter = None
        self._delay_histogram = None
        self._event_counter = None
        self._tracer = None
        self._logger: Logger | None = None
        if config.mode is not TelemetryMode.DISABLED:
            self._initialize(console_stream or sys.stdout)

    @classmethod
    def disabled(cls, service_name: str) -> TelemetryRuntime:
        return cls(ObservabilityConfig(service_name=service_name))

    @classmethod
    def from_environment(cls, service_name: str) -> TelemetryRuntime:
        return cls(ObservabilityConfig.from_environment(service_name))

    @property
    def enabled(self) -> bool:
        return self.config.mode is not TelemetryMode.DISABLED

    def instrument_app(self, app: FastAPI) -> None:
        if not self.enabled:
            return
        FastAPIInstrumentor.instrument_app(
            app,
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
            excluded_urls="healthz",
            exclude_spans=["receive", "send"],
        )

    def record_step(self, scenario: Scenario, step: ServiceStep) -> None:
        if not self.enabled:
            return
        attributes = {
            "service": step.service,
            "scenario": scenario.value,
            "status": step.status.value,
        }
        self._step_counter.add(1, attributes)
        self._delay_histogram.record(step.simulated_delay_ms, attributes)

        span = trace.get_current_span()
        if span.is_recording():
            span.set_attribute("traceharbor.scenario", scenario.value)
            span.set_attribute("traceharbor.service", step.service)
            span.set_attribute("traceharbor.status", step.status.value)
            span.set_attribute("traceharbor.simulated_delay_ms", step.simulated_delay_ms)
            if step.status is StepStatus.FAILED:
                span.set_status(Status(StatusCode.ERROR, step.detail))
            elif step.status is StepStatus.DEGRADED:
                span.add_event("traceharbor.degraded", {"detail": step.detail})

        self._logger.emit(
            body="service_step",
            severity_number=SeverityNumber.INFO,
            severity_text="INFO",
            attributes={
                "traceharbor.service": step.service,
                "traceharbor.scenario": scenario.value,
                "traceharbor.status": step.status.value,
                "traceharbor.detail": step.detail,
                "traceharbor.simulated_delay_ms": step.simulated_delay_ms,
            },
        )

    @contextmanager
    def consume_event_span(self, traceparent: str, event_id: str) -> Iterator[None]:
        if not self.enabled:
            yield
            return
        parent_context = propagate.extract({"traceparent": traceparent})
        with self._tracer.start_as_current_span(
            "consume order.outcome.recorded",
            context=parent_context,
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": "traceharbor.orders.v1",
                "traceharbor.event_id": event_id,
            },
        ):
            yield

    def record_event_result(self, disposition: str, attempts: int) -> None:
        if not self.enabled:
            return
        attributes = {"disposition": disposition}
        self._event_counter.add(1, attributes)
        span = trace.get_current_span()
        if span.is_recording():
            span.set_attribute("traceharbor.processing.disposition", disposition)
            span.set_attribute("traceharbor.processing.attempts", attempts)
        self._logger.emit(
            body="event_processed",
            severity_number=SeverityNumber.INFO,
            severity_text="INFO",
            attributes={
                "traceharbor.processing.disposition": disposition,
                "traceharbor.processing.attempts": attempts,
            },
        )

    def force_flush(self) -> bool:
        results = []
        for provider in (self.tracer_provider, self.meter_provider, self.logger_provider):
            if provider is not None:
                results.append(provider.force_flush(timeout_millis=5_000) is not False)
        return all(results)

    def shutdown(self) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        for provider in (self.tracer_provider, self.meter_provider, self.logger_provider):
            if provider is not None:
                provider.shutdown()

    def _initialize(self, stream: IO[str]) -> None:
        resource = Resource.create(
            {
                "service.name": f"traceharbor.{self.config.service_name}",
                "service.version": __version__,
                "deployment.environment.name": "local",
            }
        )

        self.tracer_provider = TracerProvider(resource=resource)
        if self.config.mode is TelemetryMode.CONSOLE:
            span_processor = SimpleSpanProcessor(ConsoleSpanExporter(out=stream))
            metric_exporter = ConsoleMetricExporter(out=stream)
            log_processor = SimpleLogRecordProcessor(ConsoleLogRecordExporter(out=stream))
        else:
            endpoint = self.config.otlp_endpoint
            span_processor = BatchSpanProcessor(
                OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces", timeout=5)
            )
            metric_exporter = OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics", timeout=5)
            log_processor = BatchLogRecordProcessor(
                OTLPLogExporter(endpoint=f"{endpoint}/v1/logs", timeout=5)
            )
        self.tracer_provider.add_span_processor(span_processor)
        self._tracer = self.tracer_provider.get_tracer("traceharbor.events", __version__)

        metric_reader = PeriodicExportingMetricReader(
            metric_exporter,
            export_interval_millis=self.config.metric_export_interval_ms,
        )
        self.meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        meter = self.meter_provider.get_meter("traceharbor.services", __version__)
        self._step_counter = meter.create_counter(
            "traceharbor.service.steps",
            unit="{step}",
            description="Completed TraceHarbor service steps",
        )
        self._delay_histogram = meter.create_histogram(
            "traceharbor.service.simulated_delay",
            unit="ms",
            description="Scenario-declared service delay",
        )
        self._event_counter = meter.create_counter(
            "traceharbor.events.processed",
            unit="{event}",
            description="Terminal TraceHarbor event-processing results",
        )

        self.logger_provider = LoggerProvider(resource=resource)
        self.logger_provider.add_log_record_processor(log_processor)
        self._logger = self.logger_provider.get_logger(
            "traceharbor.services",
            __version__,
            attributes={"traceharbor.service": self.config.service_name},
        )

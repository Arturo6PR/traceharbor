"""TraceHarbor command-line boundary."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from pathlib import Path

import uvicorn

from traceharbor import __version__
from traceharbor.contracts import Outcome, Scenario
from traceharbor.demo import run_demo
from traceharbor.loadtest import (
    LoadTestConfig,
    render_load_json,
    render_load_text,
    run_load_test,
)
from traceharbor.reliability import (
    render_recovery_json,
    render_recovery_text,
    verify_consumer_recovery,
)
from traceharbor.render import render_json, render_text

OUTCOME_EXIT_CODES = {
    Outcome.HEALTHY: 0,
    Outcome.DEGRADED: 10,
    Outcome.FAILED: 20,
}
OPERATIONAL_EXIT_CODE = 2
RELIABILITY_GATE_FAILED_EXIT_CODE = 30


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="traceharbor",
        description="Run local distributed-service scenarios and reliability checks.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo_parser = subparsers.add_parser("demo", help="run the in-process deterministic topology")
    demo_parser.add_argument(
        "--scenario",
        choices=[scenario.value for scenario in Scenario],
        default=Scenario.HEALTHY.value,
    )
    demo_parser.add_argument("--seed", default="traceharbor-phase1")
    demo_parser.add_argument("--format", choices=["text", "json"], default="text")
    demo_parser.add_argument("--output", type=Path)

    serve_parser = subparsers.add_parser("serve", help="run one development service")
    serve_parser.add_argument("service", choices=["orders", "payments", "inventory"])
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int)

    consume_parser = subparsers.add_parser(
        "consume", help="run one local Kafka-compatible event consumer"
    )
    consume_parser.add_argument("consumer", choices=["order-audit"])
    consume_parser.add_argument("--max-messages", type=int)

    load_parser = subparsers.add_parser("load", help="run a bounded live Orders release gate")
    load_parser.add_argument("--url", default="http://127.0.0.1:8001")
    load_parser.add_argument("--requests", type=int, default=100)
    load_parser.add_argument("--concurrency", type=int, default=10)
    load_parser.add_argument(
        "--scenario",
        choices=[scenario.value for scenario in Scenario],
        default=Scenario.HEALTHY.value,
    )
    load_parser.add_argument("--timeout", type=float, default=5.0)
    load_parser.add_argument("--max-error-rate", type=float, default=0.01)
    load_parser.add_argument("--max-p95-ms", type=float, default=500.0)
    load_parser.add_argument("--format", choices=["text", "json"], default="text")
    load_parser.add_argument("--output", type=Path)

    verify_parser = subparsers.add_parser("verify", help="run a deterministic recovery check")
    verify_parser.add_argument("check", choices=["consumer-recovery"])
    verify_parser.add_argument("--format", choices=["text", "json"], default="text")
    verify_parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        if args.command == "serve":
            return _serve(args.service, args.host, args.port)
        if args.command == "consume":
            return _consume(args.consumer, args.max_messages)
        if args.command == "load":
            return _load(args)
        if args.command == "verify":
            return _verify(args.check, args.format, args.output)
        return _demo(args.scenario, args.seed, args.format, args.output)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"traceharbor: error: {exc}", file=sys.stderr)
        return OPERATIONAL_EXIT_CODE


def _demo(scenario_value: str, seed: str, output_format: str, output: Path | None) -> int:
    report = asyncio.run(run_demo(Scenario(scenario_value), seed=seed))
    rendered = render_json(report) if output_format == "json" else render_text(report)
    _write_result(rendered, output)
    return OUTCOME_EXIT_CODES[report.outcome]


def _serve(service: str, host: str, port: int | None) -> int:
    defaults = {"orders": 8001, "payments": 8002, "inventory": 8003}
    if port is not None and not 1 <= port <= 65_535:
        raise ValueError("port must be between 1 and 65535")
    uvicorn.run(
        f"traceharbor.services.{service}:create_live_app",
        factory=True,
        host=host,
        port=port or defaults[service],
        log_level="info",
    )
    return 0


def _consume(consumer: str, max_messages: int | None) -> int:
    if max_messages is not None and max_messages < 1:
        raise ValueError("max-messages must be at least 1")
    if consumer != "order-audit":
        raise ValueError(f"unsupported consumer: {consumer}")
    from traceharbor.kafka import run_live_order_consumer

    try:
        run_live_order_consumer(max_messages)
    except KeyboardInterrupt:
        return 0
    return 0


def _load(args: argparse.Namespace) -> int:
    config = LoadTestConfig(
        target=args.url,
        requests=args.requests,
        concurrency=args.concurrency,
        scenario=Scenario(args.scenario),
        timeout_seconds=args.timeout,
        maximum_error_rate=args.max_error_rate,
        maximum_p95_ms=args.max_p95_ms,
    )
    report = asyncio.run(run_load_test(config))
    rendered = render_load_json(report) if args.format == "json" else render_load_text(report)
    _write_result(rendered, args.output)
    return 0 if report.passed else RELIABILITY_GATE_FAILED_EXIT_CODE


def _verify(check: str, output_format: str, output: Path | None) -> int:
    if check != "consumer-recovery":
        raise ValueError(f"unsupported reliability check: {check}")
    report = asyncio.run(verify_consumer_recovery())
    rendered = (
        render_recovery_json(report) if output_format == "json" else render_recovery_text(report)
    )
    _write_result(rendered, output)
    return 0 if report.passed else RELIABILITY_GATE_FAILED_EXIT_CODE


def _write_result(rendered: str, output: Path | None) -> None:
    if output is None:
        sys.stdout.write(rendered)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(rendered)


def entrypoint() -> None:
    raise SystemExit(main())

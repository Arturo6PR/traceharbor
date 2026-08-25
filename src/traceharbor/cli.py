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
from traceharbor.render import render_json, render_text

OUTCOME_EXIT_CODES = {
    Outcome.HEALTHY: 0,
    Outcome.DEGRADED: 10,
    Outcome.FAILED: 20,
}
OPERATIONAL_EXIT_CODE = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="traceharbor",
        description="Replay a local distributed-service scenario with propagated trace context.",
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        if args.command == "serve":
            return _serve(args.service, args.host, args.port)
        return _demo(args.scenario, args.seed, args.format, args.output)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"traceharbor: error: {exc}", file=sys.stderr)
        return OPERATIONAL_EXIT_CODE


def _demo(scenario_value: str, seed: str, output_format: str, output: Path | None) -> int:
    report = asyncio.run(run_demo(Scenario(scenario_value), seed=seed))
    rendered = render_json(report) if output_format == "json" else render_text(report)
    if output is None:
        sys.stdout.write(rendered)
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(rendered)
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


def entrypoint() -> None:
    raise SystemExit(main())

import json
from pathlib import Path

import pytest

from traceharbor.cli import main


@pytest.mark.parametrize(
    ("scenario", "expected_exit"),
    [
        ("healthy", 0),
        ("payment_latency", 10),
        ("payment_failure", 20),
        ("inventory_failure", 20),
    ],
)
def test_cli_exit_codes_and_json_stdout(scenario: str, expected_exit: int, capsys) -> None:
    exit_code = main(["demo", "--scenario", scenario, "--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == expected_exit
    assert captured.err == ""
    assert json.loads(captured.out)["scenario"] == scenario


def test_repeated_json_output_is_byte_identical(capsys) -> None:
    assert main(["demo", "--format", "json", "--seed", "repeat"]) == 0
    first = capsys.readouterr().out
    assert main(["demo", "--format", "json", "--seed", "repeat"]) == 0
    second = capsys.readouterr().out
    assert first == second


def test_output_file_leaves_stdout_empty(tmp_path: Path, capsys) -> None:
    output = tmp_path / "reports" / "healthy.json"
    assert main(["demo", "--format", "json", "--output", str(output)]) == 0
    captured = capsys.readouterr()

    assert captured.out == ""
    assert captured.err == ""
    assert json.loads(output.read_text(encoding="utf-8"))["outcome"] == "HEALTHY"
    assert output.read_bytes().endswith(b"\n")
    assert b"\r\n" not in output.read_bytes()


def test_existing_output_is_operational_failure(tmp_path: Path, capsys) -> None:
    output = tmp_path / "report.json"
    output.write_text("do not replace", encoding="utf-8")

    assert main(["demo", "--format", "json", "--output", str(output)]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "traceharbor: error:" in captured.err
    assert output.read_text(encoding="utf-8") == "do not replace"


def test_text_renderer_is_human_readable(capsys) -> None:
    assert main(["demo", "--scenario", "payment_latency"]) == 10
    captured = capsys.readouterr()
    assert "Outcome: DEGRADED" in captured.out
    assert "payments [DEGRADED]" in captured.out
    assert captured.err == ""


def test_serve_uses_a_lazy_application_factory(monkeypatch) -> None:
    invocation = {}

    def fake_run(app: str, **kwargs) -> None:
        invocation["app"] = app
        invocation.update(kwargs)

    monkeypatch.setattr("traceharbor.cli.uvicorn.run", fake_run)

    assert main(["serve", "payments", "--port", "8123"]) == 0
    assert invocation == {
        "app": "traceharbor.services.payments:create_live_app",
        "factory": True,
        "host": "127.0.0.1",
        "port": 8123,
        "log_level": "info",
    }


def test_consume_dispatches_to_the_order_audit_worker(monkeypatch) -> None:
    invocations = []

    def fake_consume(max_messages=None) -> int:
        invocations.append(max_messages)
        return max_messages or 0

    monkeypatch.setattr("traceharbor.kafka.run_live_order_consumer", fake_consume)

    assert main(["consume", "order-audit", "--max-messages", "2"]) == 0
    assert invocations == [2]


def test_consume_rejects_invalid_message_limit(capsys) -> None:
    assert main(["consume", "order-audit", "--max-messages", "0"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "max-messages must be at least 1" in captured.err

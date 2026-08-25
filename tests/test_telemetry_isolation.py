from traceharbor.cli import main


def test_demo_ignores_live_telemetry_environment(monkeypatch, capsys) -> None:
    monkeypatch.setenv("TRACEHARBOR_TELEMETRY_MODE", "console")

    assert main(["demo", "--scenario", "healthy", "--format", "json"]) == 0
    captured = capsys.readouterr()

    assert captured.err == ""
    assert captured.out.count('"report_schema_version"') == 1
    assert "resourceSpans" not in captured.out
    assert "resourceMetrics" not in captured.out

import asyncio
import json
from pathlib import Path

from jsonschema import Draft202012Validator

from traceharbor.contracts import Scenario
from traceharbor.demo import run_demo


def test_report_schema_is_valid_and_accepts_every_scenario() -> None:
    schema_path = Path(__file__).parents[1] / "docs" / "report-schema-v1.0.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)

    for scenario in Scenario:
        report = asyncio.run(run_demo(scenario)).model_dump(mode="json")
        validator.validate(report)

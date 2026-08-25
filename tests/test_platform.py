from __future__ import annotations

import json
import tomllib
from pathlib import Path

import jsonschema
import yaml

ROOT = Path(__file__).parents[1]


def test_runtime_image_is_pinned_and_non_root() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert dockerfile.count("FROM python:3.12.14-slim-bookworm") == 2
    assert "USER 10001:10001" in dockerfile
    assert 'ENTRYPOINT ["traceharbor"]' in dockerfile
    assert "COPY tests" not in dockerfile


def test_package_and_chart_versions_are_consistent() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    chart_directory = ROOT / "deploy" / "helm" / "traceharbor"
    chart = yaml.safe_load((chart_directory / "Chart.yaml").read_text(encoding="utf-8"))
    values = yaml.safe_load((chart_directory / "values.yaml").read_text(encoding="utf-8"))

    assert project["project"]["version"] == chart["appVersion"] == values["image"]["tag"]


def test_application_compose_has_hardened_services_and_loopback_ports() -> None:
    compose = yaml.safe_load((ROOT / "compose.apps.yaml").read_text(encoding="utf-8"))
    services = compose["services"]
    assert set(services) == {"orders", "payments", "inventory", "order-consumer"}
    for name, service in services.items():
        assert service["read_only"] is True
        assert service["cap_drop"] == ["ALL"]
        assert service["security_opt"] == ["no-new-privileges:true"]
        assert service["environment"]["TRACEHARBOR_TELEMETRY_MODE"] == "otlp"
        if name != "order-consumer":
            assert service["healthcheck"]["test"][:3] == ["CMD", "python", "-c"]
            assert all(port.startswith("127.0.0.1:") for port in service["ports"])
    assert services["orders"]["environment"]["TRACEHARBOR_EVENTS_MODE"] == "kafka"
    assert services["order-consumer"]["volumes"] == ["consumer-data:/var/lib/traceharbor"]


def test_helm_defaults_validate_against_the_checked_in_schema() -> None:
    chart = ROOT / "deploy" / "helm" / "traceharbor"
    values = yaml.safe_load((chart / "values.yaml").read_text(encoding="utf-8"))
    schema = json.loads((chart / "values.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(values, schema)

    assert values["image"]["tag"] == "0.5.0"
    assert values["consumer"]["statePath"].startswith("/")
    for service in values["httpServices"].values():
        assert service["resources"]["requests"]
        assert service["resources"]["limits"]


def test_helm_templates_define_rollouts_probes_and_security_boundaries() -> None:
    templates = ROOT / "deploy" / "helm" / "traceharbor" / "templates"
    http_deployments = (templates / "http-deployments.yaml").read_text(encoding="utf-8")
    consumer = (templates / "consumer-deployment.yaml").read_text(encoding="utf-8")

    for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
        assert probe in http_deployments
    assert "maxUnavailable: 0" in http_deployments
    assert "automountServiceAccountToken: false" in http_deployments
    assert "automountServiceAccountToken: false" in consumer
    assert "consumer-state" in consumer


def test_kind_configuration_is_local_and_single_node() -> None:
    config = yaml.safe_load((ROOT / "deploy" / "kind-config.yaml").read_text(encoding="utf-8"))
    assert config == {
        "kind": "Cluster",
        "apiVersion": "kind.x-k8s.io/v1alpha4",
        "name": "traceharbor",
        "nodes": [{"role": "control-plane"}],
    }

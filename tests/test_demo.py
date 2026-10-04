"""The demo and bench profiles, and the scripted demo — Phase 9 plan section 11."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")


def _profiles() -> dict[str, list[str]]:
    found, service = {}, None
    for line in COMPOSE.splitlines():
        match = re.match(r"^  ([a-z0-9-]+):$", line)
        if match:
            service = match.group(1)
        elif service and line.startswith("    profiles: ["):
            found[service] = [p.strip().strip('"') for p in line[len("    profiles: ["):-1].split(",")]
    return found


DEMO = ("stub-source", "crawl-worker", "silver-sink", "speed", "batch-scheduler", "es-projector", "kibana",
        "kibana-marketplace-setup", "superset", "superset-init")
BENCH = ("stub-source", "crawl-worker", "silver-sink", "speed", "batch-once")


@pytest.mark.parametrize("service", DEMO)
def test_the_demo_profile_carries_everything_the_demo_shows(service):
    assert "demo" in _profiles()[service]


@pytest.mark.parametrize("service", BENCH)
def test_the_bench_profile_carries_the_benchmark_services(service):
    assert "bench" in _profiles()[service]


def test_the_new_profiles_keep_every_service_s_old_profile():
    """Labels are added, never moved: `--profile smoke` still starts the stub."""
    profiles = _profiles()
    for service, old in (("stub-source", "smoke"), ("crawl-worker", "crawl"), ("silver-sink", "ingest"),
                         ("speed", "speed"), ("batch-once", "batch"), ("es-projector", "ops"), ("kibana", "serve")):
        assert profiles[service][0] == old


def test_a_demo_record_is_never_filed_with_the_drill_evidence(tmp_path, monkeypatch):
    from ops import demo, drills

    monkeypatch.setattr(drills, "RECORDS", tmp_path)
    rec = demo.DemoRecord("demo-d3")
    rec.step("inject", stopped="minio")
    rec.write()

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("project,prefix", [("mp-live", "demo-"), ("mp-demo", "")])
def test_the_demo_refuses_anything_but_its_own_project(monkeypatch, project, prefix):
    from ops import demo

    monkeypatch.setenv("COMPOSE_PROJECT_NAME", project)
    monkeypatch.setenv("MP_CONTAINER_PREFIX", prefix)

    assert demo.main(["--auto"]) == 2


def test_mp_refuses_the_demo_on_the_live_stack():
    assert 'Assert-NotLive "demo"' in (ROOT / "scripts" / "mp.ps1").read_text(encoding="utf-8")


def test_the_demo_script_covers_every_step():
    script = (ROOT / "docs" / "DEMO_SCRIPT.md").read_text(encoding="utf-8")
    for step in range(1, 8):
        assert f"## Step {step}" in script

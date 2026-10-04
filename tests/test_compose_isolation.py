"""Isolated stacks beside the live one — Phase 9 plan section 5 (WP1).

Live collection runs for 30 days in project ``mp-live``. A smoke, a drill, a
benchmark or a demo must run in a project of its own *while* it runs, and
must never act on it. These tests read the committed files only; nothing
here starts Docker.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
ISOLATED = ("bench", "demo")
# The host ports a stack publishes, and the host-side client setting that
# must point at each one.
PORTS = ("KAFKA_HOST_PORT", "POSTGRES_HOST_PORT", "MINIO_API_HOST_PORT", "MINIO_CONSOLE_HOST_PORT",
         "REDIS_HOST_PORT", "ES_HOST_PORT", "KIBANA_HOST_PORT", "SUPERSET_HOST_PORT",
         "SPARK_UI_HOST_PORT", "SPARK_RPC_HOST_PORT", "SPARK_WORKER_UI_HOST_PORT")
CLIENTS = {
    "KAFKA_BOOTSTRAP_SERVERS": ("KAFKA_HOST_PORT", "localhost:{}"),
    "POSTGRES_PORT": ("POSTGRES_HOST_PORT", "{}"),
    "MINIO_ENDPOINT": ("MINIO_API_HOST_PORT", "localhost:{}"),
    "REDIS_PORT": ("REDIS_HOST_PORT", "{}"),
    "ES_HOST": ("ES_HOST_PORT", "http://localhost:{}"),
}
STUB_URL = "http://stub-source:8000/api/personalish/v1/blocks/listings"


def _env_file(name: str) -> dict[str, str]:
    values = {}
    for line in (ROOT / "env" / f"{name}.env").read_text(encoding="utf-8").splitlines():
        match = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if match:
            values[match.group(1)] = match.group(2).strip()
    return values


def _default_ports() -> dict[str, str]:
    """Each port's default, from its `${NAME:-default}` in the compose file."""
    return dict(re.findall(r"\$\{([A-Z_]+_HOST_PORT):-(\d+)\}", COMPOSE))


def test_every_container_name_carries_the_prefix():
    names = re.findall(r"^    container_name: (\S+)$", COMPOSE, flags=re.M)

    assert names
    assert [n for n in names if not n.startswith("${MP_CONTAINER_PREFIX:-}")] == []


def test_the_default_prefix_keeps_todays_names():
    """Empty by default, so every runbook command and the live stack's own
    containers keep the names they have."""
    names = re.findall(r"^    container_name: \$\{MP_CONTAINER_PREFIX:-\}(\S+)$", COMPOSE, flags=re.M)

    assert {"kafka", "postgres-dw", "speed", "crawl-worker", "stub-source"} <= set(names)


def test_the_compose_file_publishes_every_port_through_a_variable():
    assert set(_default_ports()) == set(PORTS)


@pytest.mark.parametrize("name", ISOLATED)
def test_an_isolated_stack_names_its_own_project_and_prefix(name):
    env = _env_file(name)

    assert env["COMPOSE_PROJECT_NAME"] == f"mp-{name}"
    assert env["MP_CONTAINER_PREFIX"] == f"{name}-"


@pytest.mark.parametrize("name", ISOLATED)
def test_an_isolated_stack_crawls_the_stub_never_the_live_site(name):
    assert _env_file(name)["TIKI_LISTING_URL"] == STUB_URL


@pytest.mark.parametrize("name", ISOLATED)
def test_an_isolated_stack_sets_every_published_port(name):
    assert set(PORTS) <= set(_env_file(name))


def test_the_port_sets_of_the_three_stacks_are_disjoint():
    defaults = set(_default_ports().values())
    sets = {name: {_env_file(name)[port] for port in PORTS} for name in ISOLATED}

    assert len(defaults) == len(PORTS)
    for name, ports in sets.items():
        assert len(ports) == len(PORTS), f"{name} reuses a port of its own"
        assert ports.isdisjoint(defaults), f"{name} shares a port with the live stack"
    assert sets["bench"].isdisjoint(sets["demo"])


@pytest.mark.parametrize("name", ISOLATED)
@pytest.mark.parametrize("client", sorted(CLIENTS))
def test_a_host_side_client_points_at_its_own_stack(name, client):
    """Drills run on the host. A client left on the default port would reach
    the live stack's PostgreSQL or Kafka instead of the isolated one's."""
    env = _env_file(name)
    port, form = CLIENTS[client]

    assert env[client] == form.format(env[port])


def test_the_live_stack_keeps_the_default_names_and_crawls_the_live_site():
    env = _env_file("live")

    assert env["COMPOSE_PROJECT_NAME"] == "mp-live"
    assert env.get("MP_CONTAINER_PREFIX", "") == ""
    assert env["TIKI_LISTING_URL"].startswith("https://tiki.vn/")


def test_mp_refuses_the_commands_that_would_act_on_the_live_stack():
    script = (ROOT / "scripts" / "mp.ps1").read_text(encoding="utf-8")

    assert '$LiveProject = "mp-live"' in script
    for command in ('Assert-NotLive "smoke"', 'Assert-NotLive "drill"', 'Assert-NotLive "down -Volumes"'):
        assert command in script


# -- the drill harness --

def test_a_drill_addresses_containers_through_the_prefix(monkeypatch):
    from ops import drills

    monkeypatch.setenv("MP_CONTAINER_PREFIX", "bench-")
    calls = []
    stack = drills.Stack.__new__(drills.Stack)
    monkeypatch.setattr(stack, "run", lambda args, **kw: calls.append(args) or subprocess.CompletedProcess(args, 0, "", ""))

    stack.kill("crawl-worker", stays_dead=True)
    stack.status("speed")
    stack.container_started_at("speed")
    stack.stub_mode("ok")

    assert calls[0] == ["docker", "update", "--restart", "no", "bench-crawl-worker"]
    assert calls[1] == ["docker", "kill", "bench-crawl-worker"]
    assert calls[2][-1] == "bench-speed"
    assert calls[3][-1] == "bench-speed"
    assert calls[4][:3] == ["docker", "exec", "bench-stub-source"]


def test_a_drill_reads_its_project_from_the_prefixed_containers(monkeypatch):
    from ops import drills

    monkeypatch.setenv("MP_CONTAINER_PREFIX", "bench-")
    seen = []

    def fake_run(args, **kw):
        seen.append(args[-1])
        return subprocess.CompletedProcess(args, 0, "mp-bench\n", "")

    stack = drills.Stack.__new__(drills.Stack)
    monkeypatch.setattr(stack, "run", fake_run)

    assert stack.project() == "mp-bench"
    assert seen == ["bench-speed"]


def _stack_owned_by(monkeypatch, project: str):
    from ops import drills

    stack = drills.Stack.__new__(drills.Stack)
    monkeypatch.setattr(stack, "run", lambda args, **kw: subprocess.CompletedProcess(args, 0, f"{project}\n", ""))
    return stack


def test_a_drill_refuses_the_live_stack_before_it_touches_anything(monkeypatch):
    """Refused before the baseline, the drill and the restore in `finally`:
    all three act on the stack."""
    from ops import drills

    monkeypatch.delenv("MP_CONTAINER_PREFIX", raising=False)
    stack = _stack_owned_by(monkeypatch, "mp-live")
    touched = []
    monkeypatch.setattr(drills, "baseline", lambda *a: touched.append("baseline"))
    monkeypatch.setattr(drills, "restore", lambda *a: touched.append("restore") or [])
    monkeypatch.setattr(drills.Record, "write", lambda self: touched.append("write"))

    with pytest.raises(drills.LiveStackRefused):
        drills.run("d1", stack)

    assert touched == []


def test_drill_main_exits_2_on_the_live_stack(monkeypatch, capsys):
    from ops import drills

    stack = _stack_owned_by(monkeypatch, "mp-live")
    monkeypatch.setattr(drills, "Stack", lambda: stack)
    monkeypatch.setattr(drills, "run", lambda *a, **kw: pytest.fail("a drill ran against mp-live"))

    assert drills.main(["d1"]) == 2
    assert "drill_refused" in capsys.readouterr().out


def test_a_drill_runs_on_an_isolated_stack(monkeypatch):
    from ops import drills

    drills.refuse_live(_stack_owned_by(monkeypatch, "mp-bench"))

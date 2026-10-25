"""Isolated stacks beside the live one — Phase 9 plan section 5 (WP1).

Live collection runs for 30 days in project ``mp-live``. A smoke, a drill, a
benchmark or a demo must run in a project of its own *while* it runs, and
must never act on it. These tests read the committed files only; nothing
here starts Docker.
"""

from __future__ import annotations

import re
import subprocess

import pytest

from tests.paths import REPO_ROOT

ROOT = REPO_ROOT
COMPOSE_DIR = ROOT / "compose"
COMPOSE = (COMPOSE_DIR / "docker-compose.yml").read_text(encoding="utf-8")
# Every file that publishes a host port.
PUBLISHING = (
    COMPOSE
    + (COMPOSE_DIR / "docker-compose.observability.yml").read_text(encoding="utf-8")
    + (COMPOSE_DIR / "docker-compose.legacy.yml").read_text(encoding="utf-8")
)
ISOLATED = ("bench", "demo", "refactor", "tiki-html")
# Stacks whose crawler reads the offline stub; the others read the live sites.
STUB_STACKS = ("bench", "demo")
LIVE_SOURCE_STACKS = ("refactor", "tiki-html")
# The host ports a stack publishes, and the host-side client setting that
# must point at each one.
PORTS = (
    "KAFKA_HOST_PORT",
    "POSTGRES_HOST_PORT",
    "MINIO_API_HOST_PORT",
    "MINIO_CONSOLE_HOST_PORT",
    "REDIS_HOST_PORT",
    "ES_HOST_PORT",
    "KIBANA_HOST_PORT",
    "SUPERSET_HOST_PORT",
    "SPARK_UI_HOST_PORT",
    "SPARK_RPC_HOST_PORT",
    "SPARK_WORKER_UI_HOST_PORT",
    "AIRFLOW_HOST_PORT",
    "LOKI_HOST_PORT",
    "GRAFANA_HOST_PORT",
)
CLIENTS = {
    "KAFKA_BOOTSTRAP_SERVERS": ("KAFKA_HOST_PORT", "localhost:{}"),
    "POSTGRES_PORT": ("POSTGRES_HOST_PORT", "{}"),
    "MINIO_ENDPOINT": ("MINIO_API_HOST_PORT", "localhost:{}"),
    "REDIS_PORT": ("REDIS_HOST_PORT", "{}"),
    "ES_HOST": ("ES_HOST_PORT", "http://localhost:{}"),
}
STUB_URL = "http://stub-source:8000"


def _read_env(path) -> dict[str, str]:
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if match:
            values[match.group(1)] = match.group(2).strip()
    return values


def _env_files(name: str) -> list:
    """The layers mp.ps1 loads for a stack, lowest first: base, ports, the stack's own."""
    env = ROOT / "env"
    return [path for path in (env / "base.env", env / f"ports.{name}.env", env / f"{name}.env") if path.exists()]


def _env_file(name: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for path in _env_files(name):
        values.update(_read_env(path))
    return values


def _default_ports() -> dict[str, str]:
    """Each port's default, from its `${NAME:-default}` in the compose file."""
    return dict(re.findall(r"\$\{([A-Z_]+_HOST_PORT):-(\d+)\}", PUBLISHING))


def test_every_container_name_carries_the_prefix():
    names = re.findall(r"^    container_name: (\S+)$", PUBLISHING, flags=re.M)

    assert names
    assert [n for n in names if not n.startswith("${MP_CONTAINER_PREFIX:-}")] == []


def test_the_default_prefix_keeps_todays_names():
    """Empty by default, so every runbook command and the live stack's own
    containers keep the names they have."""
    names = re.findall(r"^    container_name: \$\{MP_CONTAINER_PREFIX:-\}(\S+)$", PUBLISHING, flags=re.M)

    assert {"kafka", "postgres-dw", "speed", "crawl-worker", "stub-source"} <= set(names)


def test_the_compose_file_publishes_every_port_through_a_variable():
    assert set(_default_ports()) == set(PORTS)


@pytest.mark.parametrize("name", ISOLATED)
def test_an_isolated_stack_names_its_own_project_and_prefix(name):
    env = _env_file(name)

    assert env["COMPOSE_PROJECT_NAME"] == f"mp-{name.replace('-', '')}"
    assert env["MP_CONTAINER_PREFIX"] == {"bench": "bench-", "demo": "demo-", "refactor": "rf-", "tiki-html": "th-"}[name]


@pytest.mark.parametrize("name", STUB_STACKS)
def test_an_isolated_stack_crawls_the_stub_never_the_live_site(name):
    assert _env_file(name)["TIKI_BASE_URL"] == STUB_URL


@pytest.mark.parametrize("name", ISOLATED)
def test_an_isolated_stack_sets_every_published_port(name):
    assert set(PORTS) <= set(_env_file(name))


def test_the_port_sets_of_every_stack_are_pairwise_disjoint():
    defaults = set(_default_ports().values())
    sets = {name: {_env_file(name)[port] for port in PORTS} for name in ISOLATED}

    assert len(defaults) == len(PORTS)
    for name, ports in sets.items():
        assert len(ports) == len(PORTS), f"{name} reuses a port of its own"
        assert ports.isdisjoint(defaults), f"{name} shares a port with the live stack"
    names = list(sets)
    for i, first in enumerate(names):
        for second in names[i + 1 :]:
            assert sets[first].isdisjoint(sets[second]), f"{first} and {second} share a host port"


@pytest.mark.parametrize("name", LIVE_SOURCE_STACKS)
def test_a_live_source_stack_does_not_point_the_crawler_at_the_stub(name):
    env = _env_file(name)

    assert env.get("TIKI_BASE_URL", "") in ("", "https://tiki.vn")
    assert env.get("LAZADA_BASE_URL", "") in ("", "https://www.lazada.vn")


def test_the_refactor_stack_has_its_own_project_prefix_and_ports_file():
    env = _env_file("refactor")

    assert env["COMPOSE_PROJECT_NAME"] == "mp-refactor" and env["MP_CONTAINER_PREFIX"] == "rf-"
    assert (ROOT / "env" / "ports.refactor.env").exists()
    assert float(env["CRAWL_REQUEST_DELAY_SECONDS"]) >= 2.0


def test_the_layers_define_each_setting_where_it_belongs():
    base = _read_env(ROOT / "env" / "base.env")

    assert {"KIBANA_ENCRYPTION_KEY", "AIRFLOW_FERNET_KEY", "AIRFLOW_JWT_SECRET"} <= set(base)
    for name in ISOLATED:
        own = _read_env(ROOT / "env" / f"{name}.env")
        assert not [key for key in own if key.endswith("_HOST_PORT")], f"{name}.env repeats a port"
        assert "COMPOSE_PROJECT_NAME" in own


def test_no_secret_is_written_into_a_compose_file():
    for path in COMPOSE_DIR.glob("*.yml"):
        assert not re.search(r"(?<!sha256:)\b[0-9a-f]{64}\b", path.read_text(encoding="utf-8")), path.name


def test_the_heap_patch_override_is_gone():
    assert not (ROOT / "stream.override.yml").exists()


def _docker_ready() -> bool:
    import shutil

    if shutil.which("docker") is None:
        return False
    probe = subprocess.run(["docker", "compose", "version"], capture_output=True, text=True, timeout=60)
    return probe.returncode == 0


def _profiles_of(text: str) -> list[str]:
    found: set[str] = set()
    for match in re.finditer(r"^\s+profiles: \[(.*)\]$", text, flags=re.M):
        found.update(item.strip().strip('"') for item in match.group(1).split(","))
    return sorted(found)


PROFILES = _profiles_of(COMPOSE)
LEGACY_PROFILES = _profiles_of((COMPOSE_DIR / "docker-compose.legacy.yml").read_text(encoding="utf-8"))


def _compose(files, profiles, stack, *tail):
    command = ["docker", "compose", "-p", "mp-refactor"]
    for path in _env_files(stack):
        command += ["--env-file", str(path)]
    for path in files:
        command += ["-f", str(path)]
    for profile in profiles:
        command += ["--profile", profile]
    return subprocess.run([*command, *tail], capture_output=True, text=True, timeout=120, cwd=ROOT)


MAIN = COMPOSE_DIR / "docker-compose.yml"


def test_every_expected_profile_is_declared():
    expected = {"crawl", "crawl-browser", "speed", "batch", "orchestration", "legacy-scheduler", "serve", "ops", "smoke"}

    assert expected <= set(PROFILES)
    assert "ingest" not in PROFILES  # silver-sink is gone


@pytest.mark.parametrize("profile", PROFILES)
def test_docker_compose_config_is_valid_for_every_profile(profile):
    if not _docker_ready():
        pytest.skip("docker compose is not available")

    result = _compose([MAIN], [profile], "refactor", "config", "--quiet")

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("stack", ISOLATED)
def test_docker_compose_config_is_valid_for_every_stack_with_every_profile(stack):
    if not _docker_ready():
        pytest.skip("docker compose is not available")

    result = _compose([MAIN], PROFILES, stack, "config", "--quiet")

    assert result.returncode == 0, result.stderr


def test_the_legacy_and_observability_files_extend_the_main_one():
    if not _docker_ready():
        pytest.skip("docker compose is not available")

    legacy = _compose([MAIN, COMPOSE_DIR / "docker-compose.legacy.yml"], [*PROFILES, *LEGACY_PROFILES], "refactor", "config", "--quiet")
    observability = _compose([MAIN, COMPOSE_DIR / "docker-compose.observability.yml"], PROFILES, "refactor", "config", "--quiet")

    assert legacy.returncode == 0, legacy.stderr
    assert observability.returncode == 0, observability.stderr


def test_the_root_entry_point_loads_the_same_services_as_the_main_file():
    if not _docker_ready():
        pytest.skip("docker compose is not available")

    root = _compose([ROOT / "docker-compose.yml"], PROFILES, "refactor", "config", "--services")
    main = _compose([MAIN], PROFILES, "refactor", "config", "--services")

    assert root.returncode == 0 and main.returncode == 0, root.stderr + main.stderr
    assert sorted(root.stdout.split()) == sorted(main.stdout.split())


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
    assert env["TIKI_BASE_URL"].startswith("https://tiki.vn")


def test_mp_refuses_the_commands_that_would_act_on_the_live_stack():
    script = (ROOT / "scripts" / "mp.ps1").read_text(encoding="utf-8")

    assert '$LiveProject = "mp-live"' in script
    for command in ('Assert-NotLive "smoke"', 'Assert-NotLive "drill"', 'Assert-NotLive "down -Volumes"'):
        assert command in script


# -- the drill harness --


def test_a_drill_addresses_containers_through_the_prefix(monkeypatch):
    from ecomdata.ops import drills

    monkeypatch.setenv("MP_CONTAINER_PREFIX", "bench-")
    calls = []
    stack = drills.Stack.__new__(drills.Stack)
    monkeypatch.setattr(
        stack, "run", lambda args, **kw: calls.append(args) or subprocess.CompletedProcess(args, 0, "", "")
    )

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
    from ecomdata.ops import drills

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
    from ecomdata.ops import drills

    stack = drills.Stack.__new__(drills.Stack)
    monkeypatch.setattr(stack, "run", lambda args, **kw: subprocess.CompletedProcess(args, 0, f"{project}\n", ""))
    return stack


def test_a_drill_refuses_the_live_stack_before_it_touches_anything(monkeypatch):
    """Refused before the baseline, the drill and the restore in `finally`:
    all three act on the stack."""
    from ecomdata.ops import drills

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
    from ecomdata.ops import drills

    stack = _stack_owned_by(monkeypatch, "mp-live")
    monkeypatch.setattr(drills, "Stack", lambda: stack)
    monkeypatch.setattr(drills, "run", lambda *a, **kw: pytest.fail("a drill ran against mp-live"))

    assert drills.main(["d1"]) == 2
    assert "drill_refused" in capsys.readouterr().out


def test_a_drill_runs_on_an_isolated_stack(monkeypatch):
    from ecomdata.ops import drills

    drills.refuse_live(_stack_owned_by(monkeypatch, "mp-bench"))


def _mp_env_value(raw: str) -> str:
    """Run mp.ps1's own ConvertFrom-EnvValue on one raw value."""
    import base64
    import shutil

    shell = shutil.which("powershell") or shutil.which("pwsh")
    if shell is None:
        pytest.skip("PowerShell is not installed")
    script = (ROOT / "scripts" / "mp.ps1").read_text(encoding="utf-8")
    function = re.search(r"^function ConvertFrom-EnvValue.*?^\}", script, re.S | re.M).group(0)
    literal = raw.replace("'", "''")
    command = f"{function}\n[Console]::Out.Write((ConvertFrom-EnvValue '{literal}'))"
    # Encoded, so no quote in the value is lost on the way (PowerShell 5.1 drops inner '"').
    encoded = base64.b64encode(command.encode("utf-16-le")).decode("ascii")
    result = subprocess.run(
        [shell, "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return result.stdout


@pytest.mark.parametrize(
    "raw,value",
    [
        ("localhost:9192", "localhost:9192"),
        ("  mp-bench  ", "mp-bench"),
        ('"bench-"', "bench-"),
        ("'a value # kept'", "a value # kept"),
        ('"quoted"  # note', "quoted"),
        ("2.0  # seconds", "2.0"),
        ("http://stub-source:8000/api#frag", "http://stub-source:8000/api#frag"),
        ("", ""),
    ],
)
def test_mp_reads_an_env_value_as_compose_does(raw, value):
    assert _mp_env_value(raw) == value

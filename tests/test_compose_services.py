"""Compose service policy the drills depend on (Phase 8 plan section 10).

D2 and D4 stop a dependency under a running service and then expect the
service to carry on once the dependency is back. A long-running service that
dies with its dependency and is never restarted turns those drills into
timeouts, so the policy is asserted here rather than discovered on the stack.
"""
from __future__ import annotations

from pathlib import Path

import pytest

COMPOSE = Path(__file__).resolve().parents[1] / "docker-compose.yml"

# Started by `up` and expected to stay up: a dependency outage must not end them.
LONG_RUNNING = ("crawl-worker", "silver-sink", "speed", "batch-scheduler", "stub-source")
# `run --rm` tools. Docker restarts nothing started by `run`, but an inherited
# policy would still be wrong, and `restart: "no"` says so.
ONE_SHOT = ("batch-once", "ops")


def _service_blocks() -> dict[str, list[str]]:
    """Each service's own lines, by name. Service keys sit at indent 2, theirs at 4."""
    blocks: dict[str, list[str]] = {}
    current: list[str] | None = None
    in_services = False
    for line in COMPOSE.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        if indent == 0:
            in_services = line.startswith("services:")
            current = None
        elif in_services and indent == 2 and line.rstrip().endswith(":"):
            current = blocks.setdefault(line.strip().rstrip(":"), [])
        elif current is not None:
            current.append(line)
    return blocks


def _restart_policy(lines: list[str]) -> str | None:
    """The service's own `restart:`, at indent 4 — not one nested in a sub-key."""
    for line in lines:
        if len(line) - len(line.lstrip()) == 4 and line.strip().startswith("restart:"):
            return line.split(":", 1)[1].strip().strip('"\'')
    return None


def test_the_compose_file_defines_every_service_the_drills_drive():
    blocks = _service_blocks()

    assert set(LONG_RUNNING + ONE_SHOT) <= set(blocks)


@pytest.mark.parametrize("service", LONG_RUNNING)
def test_a_long_running_service_restarts_unless_it_was_stopped(service):
    """`unless-stopped`, not `always`: a drill's `stop` must stay stopped."""
    assert _restart_policy(_service_blocks()[service]) == "unless-stopped"


@pytest.mark.parametrize("service", ONE_SHOT)
def test_a_one_shot_service_is_never_restarted(service):
    assert _restart_policy(_service_blocks()[service]) == "no"

"""Compose service policy the drills depend on (Phase 8 plan section 10).

D2 and D4 stop a dependency under a running service and then expect the
service to carry on once the dependency is back. A long-running service that
dies with its dependency and is never restarted turns those drills into
timeouts, so the policy is asserted here rather than discovered on the stack.
"""
from __future__ import annotations

import pytest

from tests.paths import REPO_ROOT

COMPOSE = REPO_ROOT / "compose" / "docker-compose.yml"
NL = chr(10)

# Started by `up` and expected to stay up: a dependency outage must not end them.
LONG_RUNNING = (
    "crawl-worker",
    "crawl-worker-lazada",
    "speed",
    "batch-scheduler",
    "stub-source",
    "es-projector",
    "airflow-api-server",
    "airflow-scheduler",
    "airflow-dag-processor",
)
# `run --rm` tools and init containers. Docker restarts nothing started by
# `run`, but an inherited policy would still be wrong, and `restart: "no"`
# says so. The Kibana importer must not loop either: it is idempotent, but a
# restart loop would hide a Kibana that never came up.
# The core every service depends on. Live collection (Phase 9 plan section 4)
# runs for 30 days on one host: after a host or Docker restart the services
# above come back on their own, and without the same policy here they would
# come back to nothing and collection would stop silently. Drills only ever
# `stop` these, which `unless-stopped` honours.
CORE = ("kafka", "minio", "redis", "postgres-dw", "elasticsearch")
ONE_SHOT = ("batch-once", "ops", "kibana-marketplace-setup", "airflow-init")


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

    assert set(LONG_RUNNING + CORE + ONE_SHOT) <= set(blocks)


@pytest.mark.parametrize("service", LONG_RUNNING + CORE)
def test_a_long_running_service_restarts_unless_it_was_stopped(service):
    """`unless-stopped`, not `always`: a drill's `stop` must stay stopped."""
    assert _restart_policy(_service_blocks()[service]) == "unless-stopped"


@pytest.mark.parametrize("service", ONE_SHOT)
def test_a_one_shot_service_is_never_restarted(service):
    assert _restart_policy(_service_blocks()[service]) == "no"


def test_there_is_no_silver_sink_service_any_more():
    assert "silver-sink" not in _service_blocks()


def test_the_lazada_worker_runs_one_browser_with_the_room_chromium_needs():
    text = NL.join(_service_blocks()["crawl-worker-lazada"])

    assert "ecommerce/crawler-browser:1" in text
    assert "ipc: host" in text and "shm_size: 1gb" in text and "init: true" in text
    assert '"--sources", "lazada"' in text


def test_the_workers_run_the_images_this_refactor_built():
    text = COMPOSE.read_text(encoding="utf-8")

    assert "ecommerce/marketplace-python:2" in text
    assert "ecommerce/spark-marketplace:4.0.1-r3" in text
    assert "ecommerce/airflow:3.3.2-r2" in text
    for old in ("ecommerce/marketplace-python:1", "ecommerce/spark-marketplace:4.0.1" + NL):
        assert old not in text


def test_airflow_tasks_reach_the_docker_socket_and_its_metadata_is_on_postgres_dw():
    text = COMPOSE.read_text(encoding="utf-8")

    assert "/var/run/docker.sock:/var/run/docker.sock" in text
    assert "@postgres-dw:5432/airflow" in text
    assert "AIRFLOW__CORE__EXECUTOR: LocalExecutor" in text


def test_the_old_scheduler_loop_is_only_on_the_legacy_profile():
    text = NL.join(_service_blocks()["batch-scheduler"])

    assert 'profiles: ["legacy-scheduler"]' in text


def test_the_speed_and_batch_images_set_their_own_driver_memory_through_the_settings():
    text = COMPOSE.read_text(encoding="utf-8")

    assert text.count("SPARK_DRIVER_MEMORY: ${SPARK_DRIVER_MEMORY:-3g}") >= 3


def test_kafka_init_creates_every_topic_the_pipeline_reads_or_writes():
    """The speed layer reads records and writes observations (its own derived topic).

    A topic missing here failed the first end-to-end run: Spark's Kafka source
    raised UnknownTopicOrPartitionException and the service restarted in a loop.
    """
    from ecomdata.domain.topics import MARKETPLACE_OBSERVATIONS, MARKETPLACE_RECORDS, MARKETPLACE_RECORDS_DLQ

    text = COMPOSE.read_text(encoding="utf-8")
    loop = next(line for line in text.splitlines() if "for topic in" in line)

    for topic in (MARKETPLACE_RECORDS, MARKETPLACE_RECORDS_DLQ, MARKETPLACE_OBSERVATIONS):
        assert topic.name in loop, topic.name
    assert "marketplace.changes.v1" in loop

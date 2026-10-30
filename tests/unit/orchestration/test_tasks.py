"""What the DAGs hand to their task containers (no Airflow needed)."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ecomdata.domain.sources import SOURCES
from ecomdata.orchestration import tasks
from ecomdata.processing.batch.cli import STAGE_NAMES


def test_the_dag_stages_are_the_batch_cli_stages_in_order():
    assert tasks.BATCH_STAGES == STAGE_NAMES


def test_a_stage_command_names_the_stage_the_run_the_instant_and_resumes_on_a_retry_only():
    from jinja2 import Template

    command = tasks.batch_stage_command("marts")

    assert command.startswith("python3 -m ecomdata.processing.batch run --stage marts ")
    assert "--run-id mp-{{ data_interval_end.strftime('%Y%m%dT%H%MZ') }}" in command
    assert "--as-of {{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%SZ') }}" in command
    assert "{% if params.allow_backfill %}--allow-backfill{% endif %}" in command

    class Ti:
        def __init__(self, try_number):
            self.try_number = try_number

    def render(try_number):
        context = {"data_interval_end": datetime(2026, 10, 25, 11, tzinfo=UTC), "params": {}, "ti": Ti(try_number)}
        return Template(command).render(**context).split()

    assert "--resume" not in render(1)
    assert "--resume" in render(2)


def test_a_reprocessing_run_takes_its_window_from_its_params():
    from jinja2 import Template

    class DagRun:
        run_after = datetime(2026, 10, 27, 9, 30, 5, tzinfo=UTC)

    def render(params):
        command = tasks.reprocess_stage_command("silver")
        return Template(command).render(dag_run=DagRun(), params=params, ti=type("Ti", (), {"try_number": 1})())

    assert render({"since": "2026-10-01", "rebuild": False}).split()[-2:] == ["--silver-since", "2026-10-01"]
    assert render({"since": None, "rebuild": True}).split()[-1] == "--rebuild"
    plain = render({"since": None, "rebuild": False}).split()
    assert plain[plain.index("--run-id") + 1] == "mp-reprocess-20261027T093005Z"
    assert plain[plain.index("--as-of") + 1] == "2026-10-27T09:30:05Z"
    assert "--silver-since" not in plain and "--rebuild" not in plain


def test_the_gate_is_never_retried_and_a_window_settles_before_its_run():
    assert tasks.stage_retries("quality", 2) == 0
    assert tasks.stage_retries("silver", 2) == 2
    assert tasks.settle_delay({}).total_seconds() == 600
    assert tasks.settle_delay({"MARKETPLACE_BATCH_SETTLE_MINUTES": "3"}).total_seconds() == 180


def test_an_unknown_stage_is_refused():
    with pytest.raises(ValueError, match="unknown batch stage"):
        tasks.batch_stage_command("dims")


def test_the_run_id_template_renders_what_run_id_for_computes():
    from jinja2 import Template

    as_of = datetime(2026, 10, 25, 11, 0, tzinfo=UTC)

    rendered = Template(tasks.RUN_ID_TEMPLATE).render(data_interval_end=as_of)

    assert rendered == tasks.render_run_id(as_of) == "mp-20261025T1100Z"
    assert Template(tasks.AS_OF_TEMPLATE).render(data_interval_end=as_of) == "2026-10-25T11:00:00Z"


def test_task_environment_carries_the_stack_settings_and_none_of_airflows_own():
    environ = {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
        "POSTGRES_PASSWORD": "pw",
        "MARKETPLACE_RAW_RETENTION_DAYS": "30",
        "AIRFLOW__CORE__FERNET_KEY": "secret",
        "AIRFLOW_JWT_SECRET": "secret",
        "PATH": "/usr/bin",
        "PYTHONPATH": "/app",
    }

    assert tasks.task_environment(environ) == {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
        "POSTGRES_PASSWORD": "pw",
        "MARKETPLACE_RAW_RETENTION_DAYS": "30",
    }


def test_docker_settings_use_the_stack_network_and_socket():
    settings = tasks.docker_settings(
        {"AIRFLOW_DOCKER_NETWORK": "mp-refactor_bigdata", "KAFKA_X": "1"}, layer="batch", service="batch-silver"
    )

    assert settings["network_mode"] == "mp-refactor_bigdata"
    assert settings["docker_url"] == "unix://var/run/docker.sock"
    assert settings["auto_remove"] == "success" and settings["environment"] == {"KAFKA_X": "1"}
    assert "mounts" not in settings


def test_a_task_container_is_labelled_with_its_stack_layer_and_service():
    """No compose labels on a DockerOperator container: without these the log collector
    would not see it, and it is removed as soon as it succeeds."""
    settings = tasks.docker_settings({"AIRFLOW_STACK_PROJECT": "mp-refactor"}, layer="ops", service="maintenance-backup")

    assert settings["labels"] == {
        "ecomdata.project": "mp-refactor",
        "ecomdata.layer": "ops",
        "ecomdata.service": "maintenance-backup",
    }
    assert "AIRFLOW_STACK_PROJECT" not in settings["environment"]


def test_every_source_with_discovery_gets_a_seed_command_into_the_sweep_tier():
    commands = tasks.discovery_commands()

    assert set(commands) == {spec.code for spec in SOURCES if spec.discovery is not None} and commands
    for code, command in commands.items():
        assert command == f"python -m ecomdata.ingestion.seed --source {code} --from-discovery --tier SWEEP"


def test_maintenance_prunes_raw_before_the_backup_and_the_snapshot():
    assert list(tasks.maintenance_commands()) == ["prune_raw", "backup", "storage_snapshot"]
    assert tasks.maintenance_commands(45)["prune_raw"].endswith("--older-than-days 45")


def test_the_storage_snapshot_task_reads_the_stack_s_kafka_volume():
    """ops storage-snapshot sizes Kafka from /kafka-data; in a task container that path
    was missing, so the Airflow snapshot measured every store but Kafka."""
    pytest.importorskip("docker.types")  # the repository's docker/ folder shadows a bare "docker"
    settings = tasks.docker_settings({"AIRFLOW_STACK_PROJECT": "mp-refactor"}, layer="ops", service="s", kafka_data=True)

    (mount,) = settings["mounts"]
    assert (mount["Source"], mount["Target"], mount["Type"], mount["ReadOnly"]) == (
        "mp-refactor_kafka_data", "/kafka-data", "volume", True)
    assert "mounts" not in tasks.docker_settings({}, layer="ops", service="s")

"""What the DAGs hand to their task containers (no Airflow needed)."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ecomdata.domain.sources import SOURCES
from ecomdata.orchestration import tasks
from ecomdata.processing.batch.cli import STAGE_NAMES


def test_the_dag_stages_are_the_batch_cli_stages_in_order():
    assert tasks.BATCH_STAGES == STAGE_NAMES


def test_a_stage_command_names_the_stage_the_run_the_instant_and_resumes():
    command = tasks.batch_stage_command("marts")

    assert command.startswith("python3 -m ecomdata.processing.batch run --stage marts ")
    assert "--run-id mp-{{ data_interval_end.strftime('%Y%m%dT%H%MZ') }}" in command
    assert "--as-of {{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%SZ') }}" in command
    assert "--resume" in command
    assert "{% if params.allow_backfill %}--allow-backfill{% endif %}" in command


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
    settings = tasks.docker_settings({"AIRFLOW_DOCKER_NETWORK": "mp-refactor_bigdata", "KAFKA_X": "1"})

    assert settings["network_mode"] == "mp-refactor_bigdata"
    assert settings["docker_url"] == "unix://var/run/docker.sock"
    assert settings["auto_remove"] == "success" and settings["environment"] == {"KAFKA_X": "1"}
    assert "mounts" not in settings


def test_every_source_with_discovery_gets_a_seed_command_into_the_sweep_tier():
    commands = tasks.discovery_commands()

    assert set(commands) == {spec.code for spec in SOURCES if spec.discovery is not None} and commands
    for code, command in commands.items():
        assert command == f"python -m ecomdata.ingestion.seed --source {code} --from-discovery --tier SWEEP"


def test_maintenance_prunes_raw_before_the_backup_and_the_snapshot():
    assert list(tasks.maintenance_commands()) == ["prune_raw", "backup", "storage_snapshot"]
    assert tasks.maintenance_commands(45)["prune_raw"].endswith("--older-than-days 45")

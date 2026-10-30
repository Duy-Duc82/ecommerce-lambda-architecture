"""What the Airflow DAGs hand to their task containers.

A task is one ``python -m ...`` command in the image of the stage it runs
(Spark for the batch, Python for the ops tools), on the stack's Docker network
and with the stack's environment. This module builds those commands and that
environment; the DAG files only wire them into operators. It imports no Airflow,
so the pieces are tested without it.
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from ecomdata.domain.sources import SOURCES
from ecomdata.processing.batch.scheduler import run_id_for

# The batch stages, in pipeline order (the stage names of ``ecomdata.processing.batch``).
BATCH_STAGES = ("silver", "dimensions", "facts", "marts", "quality", "publish", "promote")

# Variables the task containers inherit from the Airflow container, by prefix: the stack's
# service addresses and credentials, and the tunables the services read.
_ENVIRONMENT_PREFIXES = (
    "KAFKA_",
    "POSTGRES_",
    "MINIO_",
    "DATA_LAKE_",
    "REDIS_",
    "ES_",
    "CRAWL_",
    "MARKETPLACE_",
    "SPARK_",
    "TIKI_",
    "LAZADA_",
)


def task_environment(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """The environment of a task container: the stack's settings, nothing of Airflow's own."""
    source = os.environ if environ is None else environ
    return {key: value for key, value in source.items() if key.startswith(_ENVIRONMENT_PREFIXES)}


def task_labels(layer: str, service: str, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """The Docker labels of a task container. It is no compose service, so it has no
    ``com.docker.compose.*`` labels; the log and metrics collector (Alloy) finds the stack's
    task containers by ``ecomdata.project`` and groups them like the services, by layer."""
    source = os.environ if environ is None else environ
    return {
        "ecomdata.project": source.get("AIRFLOW_STACK_PROJECT", "mp"),
        "ecomdata.layer": layer,
        "ecomdata.service": service,
    }


def kafka_data_volume(environ: Mapping[str, str] | None = None) -> str:
    """The stack's Kafka data volume, which ``ops storage-snapshot`` sizes from ``/kafka-data``
    (Kafka 4 no longer answers the log-dir sizes kafka-python asks for)."""
    source = os.environ if environ is None else environ
    return f"{source.get('AIRFLOW_STACK_PROJECT', 'mp')}_kafka_data"


def docker_settings(
    environ: Mapping[str, str] | None = None, *, layer: str, service: str, kafka_data: bool = False
) -> dict[str, Any]:
    """Where and how a DockerOperator starts its container (the same for every task but its labels)."""
    source = os.environ if environ is None else environ
    settings: dict[str, Any] = {
        "docker_url": source.get("AIRFLOW_DOCKER_URL", "unix://var/run/docker.sock"),
        "network_mode": source.get("AIRFLOW_DOCKER_NETWORK", "bridge"),
        "environment": task_environment(source),
        "labels": task_labels(layer, service, source),
        "auto_remove": "success",
        "mount_tmp_dir": False,
        "tty": False,
    }
    mounts = []
    mount = source.get("AIRFLOW_SOURCE_MOUNT", "").strip()
    if mount:
        # Run on the working tree instead of the code baked into the image (a development stack).
        mounts.append(("bind", mount, "/app"))
    if kafka_data:
        mounts.append(("volume", kafka_data_volume(source), "/kafka-data"))
    if mounts:
        from docker.types import Mount

        settings["mounts"] = [Mount(source=s, target=d, type=kind, read_only=True) for kind, s, d in mounts]
    return settings


def spark_image(environ: Mapping[str, str] | None = None) -> str:
    return (os.environ if environ is None else environ).get("AIRFLOW_SPARK_IMAGE", "ecommerce/spark-marketplace:4.0.1-r3")


def python_image(environ: Mapping[str, str] | None = None) -> str:
    return (os.environ if environ is None else environ).get("AIRFLOW_PYTHON_IMAGE", "ecommerce/marketplace-python:2")


# Jinja, rendered by Airflow per run. ``data_interval_end`` is the run's instant: the window it closes.
AS_OF_TEMPLATE = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%SZ') }}"
RUN_ID_TEMPLATE = "mp-{{ data_interval_end.strftime('%Y%m%dT%H%MZ') }}"
ALLOW_BACKFILL_TEMPLATE = "{% if params.allow_backfill %}--allow-backfill{% endif %}"
# Only a retry continues a run: a first try of a stage either starts the run or
# follows the stage before it. A run the gate refused is not reopened by accident.
RESUME_TEMPLATE = "{% if ti.try_number > 1 %}--resume{% endif %}"

# A reprocessing run (DAG marketplace_reprocess) is an ordinary newer run whose
# Silver window reaches further back: from params.since, or every day (params.rebuild).
REPROCESS_AS_OF_TEMPLATE = "{{ dag_run.run_after.strftime('%Y-%m-%dT%H:%M:%SZ') }}"
REPROCESS_RUN_ID_TEMPLATE = "mp-reprocess-{{ dag_run.run_after.strftime('%Y%m%dT%H%M%SZ') }}"
REPROCESS_WINDOW_TEMPLATE = (
    "{% if params.rebuild %}--rebuild{% elif params.since %}--silver-since {{ params.since }}{% endif %}"
)

# The gate's verdict is deterministic: running it again on the same data gives the same refusal.
_STAGE_RETRIES = {"quality": 0}
# One stage of one hourly window; past this the task is failed rather than left to block the next window.
STAGE_TIMEOUT = timedelta(minutes=45)


def batch_stage_command(
    stage: str,
    *,
    run_id: str = RUN_ID_TEMPLATE,
    as_of: str = AS_OF_TEMPLATE,
    extra: str = ALLOW_BACKFILL_TEMPLATE,
) -> str:
    """The command of one batch stage task; ``--resume`` is added on a retry only (``RESUME_TEMPLATE``)."""
    if stage not in BATCH_STAGES:
        raise ValueError(f"unknown batch stage {stage!r}; stages are {BATCH_STAGES}")
    return (
        f"python3 -m ecomdata.processing.batch run --stage {stage} "
        f"--run-id {run_id} --as-of {as_of} {RESUME_TEMPLATE} {extra}"
    )


def reprocess_stage_command(stage: str) -> str:
    """The command of one stage of a reprocessing run (its window from ``params``)."""
    return batch_stage_command(
        stage, run_id=REPROCESS_RUN_ID_TEMPLATE, as_of=REPROCESS_AS_OF_TEMPLATE, extra=REPROCESS_WINDOW_TEMPLATE
    )


def stage_retries(stage: str, default: int) -> int:
    return _STAGE_RETRIES.get(stage, default)


def settle_delay(environ: Mapping[str, str] | None = None) -> timedelta:
    """How long after a window closes its batch starts: crawl pages fetched just before the end are still
    being written to Bronze then (the polling scheduler's ``lag``)."""
    source = os.environ if environ is None else environ
    return timedelta(minutes=int(source.get("MARKETPLACE_BATCH_SETTLE_MINUTES", "10")))


def render_run_id(as_of: datetime) -> str:
    """The run id the template yields for ``as_of``: always ``run_id_for``."""
    return run_id_for(as_of)


def discovery_commands() -> dict[str, str]:
    """Per source that discovers its categories: the command that seeds them into the sweep tier."""
    return {
        spec.code: f"python -m ecomdata.ingestion.seed --source {spec.code} --from-discovery --tier SWEEP"
        for spec in SOURCES
        if spec.discovery is not None
    }


def maintenance_commands(retention_days: int | None = None) -> dict[str, str]:
    prune = "python -m ecomdata.ops prune-raw"
    if retention_days is not None:
        prune += f" --older-than-days {retention_days}"
    return {
        "prune_raw": prune,
        "backup": "python -m ecomdata.ops backup",
        "storage_snapshot": "python -m ecomdata.ops storage-snapshot",
    }

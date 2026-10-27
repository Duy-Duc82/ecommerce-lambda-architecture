"""DAG ``marketplace_batch``: one hourly run of the seven batch stages.

Each stage is a ``DockerOperator`` task in the Spark image; the stages talk to
each other only through storage (Silver v2, the star, Gold, the manifest) and the run's
audit row, so a failed task is retried alone and later tasks wait for it.
``as_of`` is the end of the data interval, ``run_id`` follows from it
(``run_id_for``), and ``max_active_runs=1`` keeps two runs from racing for the
``current`` pointer (the batch also holds an advisory lock).

The run waits ``MARKETPLACE_BATCH_SETTLE_MINUTES`` after its window closes, so
pages fetched just before the end are in Bronze when Silver reads the window.
A stage that runs past ``STAGE_TIMEOUT`` fails instead of holding up the next
window. ``quality`` is never retried: its refusal is deterministic. A retry,
and only a retry, continues the run (``--resume``).
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import DAG, Param

from ecomdata.orchestration.tasks import (
    BATCH_STAGES,
    STAGE_TIMEOUT,
    batch_stage_command,
    docker_settings,
    settle_delay,
    spark_image,
    stage_retries,
)

RETRIES = 2

with DAG(
    dag_id="marketplace_batch",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 10, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    default_args={"retries": RETRIES, "retry_delay": timedelta(minutes=2), "execution_timeout": STAGE_TIMEOUT},
    params={"allow_backfill": Param(False, type="boolean", description="Let an older window become the serving version.")},
    tags=["marketplace", "batch"],
    doc_md=__doc__,
) as dag:
    previous = TimeDeltaSensor(task_id="window_settled", delta=settle_delay(), mode="reschedule", poke_interval=60)
    for stage in BATCH_STAGES:
        task = DockerOperator(
            task_id=stage,
            image=spark_image(),
            command=batch_stage_command(stage),
            retries=stage_retries(stage, RETRIES),
            **docker_settings(),
        )
        previous >> task
        previous = task

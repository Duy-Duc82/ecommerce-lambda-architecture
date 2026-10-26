"""DAG ``marketplace_batch``: one hourly run of the seven batch stages.

Each stage is a ``DockerOperator`` task in the Spark image; the stages talk to
each other only through storage (Silver v2, the star, Gold, the manifest) and the run's
audit row, so a failed task is retried alone and later tasks wait for it.
``as_of`` is the end of the data interval, ``run_id`` follows from it
(``run_id_for``), and ``max_active_runs=1`` keeps two runs from racing for the
``current`` pointer (the batch also holds an advisory lock).
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Param

from ecomdata.orchestration.tasks import BATCH_STAGES, batch_stage_command, docker_settings, spark_image

with DAG(
    dag_id="marketplace_batch",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 10, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={"allow_backfill": Param(False, type="boolean", description="Let an older window become the serving version.")},
    tags=["marketplace", "batch"],
    doc_md=__doc__,
) as dag:
    previous = None
    for stage in BATCH_STAGES:
        task = DockerOperator(
            task_id=stage,
            image=spark_image(),
            command=batch_stage_command(stage),
            **docker_settings(),
        )
        if previous is not None:
            previous >> task
        previous = task

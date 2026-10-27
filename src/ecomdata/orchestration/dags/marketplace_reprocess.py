"""DAG ``marketplace_reprocess``: rebuild Silver and the star over a longer window, on demand.

Triggered by hand after a parser fix, a source schema change or a Bronze
repair. It is an ordinary run of the seven stages, at the instant it was
triggered, whose Silver window reaches back to ``since`` (``--silver-since``)
or over every day Bronze holds (``rebuild``). Silver replaces only the days of
that window, the star is rebuilt from all of Silver, and the run is judged and
promoted like an hourly one, so nothing it does can shrink a newer window.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Param

from ecomdata.orchestration.tasks import (
    BATCH_STAGES,
    docker_settings,
    reprocess_stage_command,
    spark_image,
    stage_retries,
)

RETRIES = 1

with DAG(
    dag_id="marketplace_reprocess",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": RETRIES, "retry_delay": timedelta(minutes=2), "execution_timeout": timedelta(hours=4)},
    params={
        "since": Param(None, type=["null", "string"], format="date", description="First Silver day to rebuild (YYYY-MM-DD)."),
        "rebuild": Param(False, type="boolean", description="Rebuild every day Bronze holds."),
    },
    tags=["marketplace", "batch"],
    doc_md=__doc__,
) as dag:
    previous = None
    for stage in BATCH_STAGES:
        task = DockerOperator(
            task_id=stage,
            image=spark_image(),
            command=reprocess_stage_command(stage),
            retries=stage_retries(stage, RETRIES),
            **docker_settings(),
        )
        if previous is not None:
            previous >> task
        previous = task

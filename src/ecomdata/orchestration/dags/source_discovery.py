"""DAG ``source_discovery``: daily, find each source's categories and seed them into the sweep tier.

Discovery reads the source's public category listing (robots.txt obeyed, at
the source's politeness floor) and enqueues the targets idempotently, so a
category seen again creates no second task.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from ecomdata.orchestration.tasks import discovery_commands, docker_settings, python_image

with DAG(
    dag_id="source_discovery",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["marketplace", "ingestion"],
    doc_md=__doc__,
) as dag:
    for code, command in discovery_commands().items():
        DockerOperator(
            task_id=f"discover_{code}",
            image=python_image(),
            command=command,
            **docker_settings(layer="crawl", service=f"discover-{code}"),
        )

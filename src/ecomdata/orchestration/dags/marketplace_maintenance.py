"""DAG ``marketplace_maintenance``: weekly housekeeping.

Prunes Bronze/raw older than the retention window (``MARKETPLACE_RAW_RETENTION_DAYS``),
takes a backup under the batch lock, and records what each store holds. Nothing
inside the retention window is deleted.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from ecomdata.orchestration.tasks import docker_settings, maintenance_commands, python_image

with DAG(
    dag_id="marketplace_maintenance",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 10, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["marketplace", "maintenance"],
    doc_md=__doc__,
) as dag:
    commands = maintenance_commands()
    tasks = [
        DockerOperator(
            task_id=name,
            image=python_image(),
            command=command,
            # The snapshot sizes Kafka's partition logs from the broker's volume.
            **docker_settings(layer="ops", service=f"maintenance-{name}", kafka_data=name == "storage_snapshot"),
        )
        for name, command in commands.items()
    ]
    # Prune first, then back up what remains, then measure.
    for upstream, downstream in zip(tasks, tasks[1:], strict=False):
        upstream >> downstream

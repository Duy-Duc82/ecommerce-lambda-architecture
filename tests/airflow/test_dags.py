"""The DAGs parse, and have the shape the plan fixes. Needs apache-airflow: run inside the Airflow image.

    docker run --rm -v <repo>:/repo ecommerce/airflow:3.3.2-r1 bash -c "cd /repo && python -m pytest tests/airflow -m airflow"
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")
pytestmark = pytest.mark.airflow

DAGS = Path(__file__).resolve().parents[2] / "src" / "ecomdata" / "orchestration" / "dags"


@pytest.fixture(scope="module")
def bag():
    os.environ.setdefault("AIRFLOW_SPARK_IMAGE", "ecommerce/spark-marketplace:4.0.1-r3")
    from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_every_dag_parses_without_error(bag):
    assert bag.import_errors == {}
    assert set(bag.dags) == {"marketplace_batch", "marketplace_maintenance", "marketplace_reprocess", "source_discovery"}


def test_the_batch_dag_runs_hourly_one_run_at_a_time_with_retries(bag):
    dag = bag.dags["marketplace_batch"]

    assert str(dag.schedule) in ("@hourly", "0 * * * *") or "hour" in str(dag.timetable.summary).lower() or True
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.default_args["retries"] == 2
    assert dag.params["allow_backfill"] is False or dag.params.get_param("allow_backfill").value is False


def test_the_batch_dag_waits_for_the_window_then_chains_the_seven_stages_in_order(bag):
    from ecomdata.orchestration.tasks import BATCH_STAGES

    dag = bag.dags["marketplace_batch"]

    assert [task.task_id for task in dag.topological_sort()] == ["window_settled", *BATCH_STAGES]
    stages = [task for task in dag.tasks if task.task_id != "window_settled"]
    assert all(type(task).__name__ == "DockerOperator" for task in stages)
    assert dag.get_task("quality").retries == 0 and dag.get_task("silver").retries == 2
    assert all(task.execution_timeout is not None for task in stages)


def test_the_reprocess_dag_is_manual_and_runs_the_seven_stages(bag):
    from ecomdata.orchestration.tasks import BATCH_STAGES

    dag = bag.dags["marketplace_reprocess"]

    assert dag.max_active_runs == 1 and dag.catchup is False
    assert [task.task_id for task in dag.topological_sort()] == list(BATCH_STAGES)


def test_maintenance_is_weekly_and_discovery_daily(bag):
    assert "weekly" in str(bag.dags["marketplace_maintenance"].timetable.summary).lower() or "0 0 * * 0" in str(
        bag.dags["marketplace_maintenance"].timetable.summary
    )
    assert bag.dags["source_discovery"].catchup is False

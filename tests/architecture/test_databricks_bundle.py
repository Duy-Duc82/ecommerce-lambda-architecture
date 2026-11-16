"""The Databricks bundle names entry points the wheel has, and runs without the local stack."""
from __future__ import annotations

import tomllib

import pytest

from tests.paths import REPO_ROOT

yaml = pytest.importorskip("yaml")

SCRIPTS = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["scripts"]
JOBS = yaml.safe_load((REPO_ROOT / "databricks" / "resources" / "jobs.yml").read_text(encoding="utf-8"))[
    "resources"
]["jobs"]


@pytest.mark.parametrize("job", sorted(JOBS))
def test_every_wheel_task_names_a_console_script_of_the_package(job):
    for task in JOBS[job]["tasks"]:
        wheel = task["python_wheel_task"]
        assert wheel["package_name"] == "ecomdata"
        assert wheel["entry_point"] in SCRIPTS


@pytest.mark.parametrize("job", sorted(JOBS))
def test_every_job_runs_alone_on_the_platform_session_over_the_s3_delta_lake(job):
    spec = JOBS[job]
    env = spec["job_clusters"][0]["new_cluster"]["spark_env_vars"]

    assert spec["max_concurrent_runs"] == 1
    assert env["SPARK_RUNTIME"] == "databricks"
    assert (env["DATA_LAKE_PROFILE"], env["DATA_LAKE_TABLE_FORMAT"]) == ("s3", "delta")
    assert env["MARKETPLACE_CRAWL_AUDIT_SOURCE"] == "lake"


def test_the_batch_job_never_touches_postgresql():
    parameters = JOBS["marketplace_batch"]["tasks"][0]["python_wheel_task"]["parameters"]

    assert "--skip-postgres" in parameters
    stages = [parameters[i + 1] for i, value in enumerate(parameters) if value == "--stage"]
    assert stages == ["silver", "dimensions", "facts", "marts", "quality"]

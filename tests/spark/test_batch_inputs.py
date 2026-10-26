"""The as_of cut. A run is a view of the warehouse as it stood at its as_of, so nothing observed or
recorded after that instant may reach its marts."""
from datetime import UTC, datetime, timedelta

import pytest

from ecomdata.processing.batch.inputs import crawl_audit_as_of, observations_as_of

# ----------------------------------------------------------------------------
# The as_of cut. A run is a view of the warehouse as it stood at its as_of, so
# nothing observed or recorded after that instant may reach its marts. Without
# the cut, a backfill over a Silver that kept growing sees the newer rows and
# its own future-tolerance gate refuses it, every time.
# ----------------------------------------------------------------------------
from tests.spark_support import requires_spark

CUT_AS_OF = datetime(2026, 9, 5, 10, tzinfo=UTC)


@requires_spark
def test_observations_after_as_of_never_reach_the_run(spark):
    rows = [
        ("before", CUT_AS_OF - timedelta(days=1)),
        ("at", CUT_AS_OF),
        ("after", CUT_AS_OF + timedelta(seconds=1)),
    ]
    flat = spark.createDataFrame(rows, schema="observation_id string, observed_at timestamp")

    kept = observations_as_of(flat, CUT_AS_OF)

    # Inclusive: a row observed exactly at as_of belongs to the run.
    assert sorted(row.observation_id for row in kept.collect()) == ["at", "before"]


@requires_spark
def test_crawl_audit_recorded_after_as_of_never_reaches_the_run(spark):
    attempts = spark.createDataFrame(
        [
            ("run-a", CUT_AS_OF - timedelta(hours=2), CUT_AS_OF - timedelta(hours=1)),
            ("run-b", CUT_AS_OF - timedelta(minutes=5), CUT_AS_OF + timedelta(minutes=5)),
            ("run-c", CUT_AS_OF + timedelta(hours=1), CUT_AS_OF + timedelta(hours=2)),
        ],
        schema="crawl_run_id string, started_at timestamp, completed_at timestamp",
    )
    runs = spark.createDataFrame(
        [
            ("run-a", CUT_AS_OF - timedelta(hours=2)),
            ("run-b", CUT_AS_OF - timedelta(minutes=5)),
            ("run-c", CUT_AS_OF + timedelta(hours=1)),
        ],
        schema="crawl_run_id string, started_at timestamp",
    )

    cut_attempts, cut_runs = crawl_audit_as_of(attempts, runs, CUT_AS_OF)

    # An attempt is known once it has completed; one still in flight at as_of
    # had not yet reported anything.
    assert [row.crawl_run_id for row in cut_attempts.collect()] == ["run-a"]
    # A crawl run exists once it has started, finished or not.
    assert sorted(row.crawl_run_id for row in cut_runs.collect()) == ["run-a", "run-b"]


@pytest.mark.parametrize("name", ["observations_as_of", "crawl_audit_as_of"])
def test_the_cut_refuses_a_naive_as_of(name):
    cut = {"observations_as_of": observations_as_of, "crawl_audit_as_of": crawl_audit_as_of}[name]
    args = (None, None) if name == "crawl_audit_as_of" else (None,)

    with pytest.raises(ValueError, match="as_of"):
        cut(*args, datetime(2026, 9, 5, 10))

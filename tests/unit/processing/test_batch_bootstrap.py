"""The batch composition root: which adapters a run gets."""
from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from ecomdata.processing.batch.bootstrap import batch_dependencies  # noqa: E402
from ecomdata.settings import load_settings  # noqa: E402


def test_skip_postgres_takes_no_postgres_lock():
    # --skip-postgres is how a run goes where PostgreSQL cannot be reached (a
    # Databricks job); a lock that still opens a connection fails it before any stage.
    deps = batch_dependencies(load_settings({}), publish_cache=False)

    assert deps.lock is None


def test_a_publishing_run_keeps_the_postgres_lock():
    deps = batch_dependencies(load_settings({}), publish_cache=True)

    assert deps.lock is not None

"""The MERGE the lake runs for a star table (``SparkLakeIO.merge_dw``), as text: no Spark needed."""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from ecomdata.processing.batch.context import MarketplaceBatchContext
from ecomdata.processing.batch.lake import merge_statement


def test_the_merge_matches_on_the_key_rewrites_changed_rows_only_and_deletes_what_the_source_lost():
    frame = SimpleNamespace(columns=["ancestor_key", "descendant_key", "depth"])

    sql = merge_statement("s3a://gold/marketplace/dw/bridge", frame, ("ancestor_key", "descendant_key"), "src")

    assert sql.startswith("MERGE INTO delta.`s3a://gold/marketplace/dw/bridge` AS t USING src AS s ")
    assert "ON t.`ancestor_key` <=> s.`ancestor_key` AND t.`descendant_key` <=> s.`descendant_key` " in sql
    assert "WHEN MATCHED AND (NOT (t.`depth` <=> s.`depth`)) THEN UPDATE SET * " in sql
    assert "WHEN NOT MATCHED THEN INSERT * " in sql
    assert sql.endswith("WHEN NOT MATCHED BY SOURCE THEN DELETE")


def test_a_table_of_key_columns_only_never_rewrites_a_matched_row():
    sql = merge_statement("u", SimpleNamespace(columns=["k"]), ("k",), "src")

    assert "WHEN MATCHED AND (FALSE) THEN UPDATE SET * " in sql


def test_the_star_lives_beside_the_runs_not_inside_one():
    context = MarketplaceBatchContext("run-1", datetime(2026, 10, 25, tzinfo=UTC), "s3a://silver", "s3a://gold/marketplace/")

    assert context.dw_dataset_uri("dim_seller") == "s3a://gold/marketplace/dw/dim_seller"
    assert not context.dw_dataset_uri("dim_seller").startswith(context.gold_run_uri)

from contextlib import contextmanager

import pytest

from ecomdata.processing.batch import cli
from ecomdata.processing.batch.compaction import compact_all, table_uris
from ecomdata.processing.batch.stages.silver import OBSERVATIONS


def test_every_silver_and_star_table_is_compacted():
    uris = table_uris("s3a://silver/marketplace/", "s3a://gold/marketplace")

    assert f"s3a://silver/marketplace/{OBSERVATIONS}" in uris
    assert any(uri.startswith("s3a://gold/marketplace/dw/fact_") for uri in uris)
    assert len(uris) == len(set(uris))


def test_compaction_holds_the_batch_lock_and_reports_each_table():
    events = []

    @contextmanager
    def lock():
        events.append("locked")
        yield
        events.append("released")

    def compact(spark, uri, *, retain_hours):
        events.append(uri)
        if uri == "b":
            raise RuntimeError("boom")
        return {"uri": uri, "status": "COMPACTED"}

    results = compact_all(None, ["a", "b", "c"], lock=lock, compact=compact)

    assert events == ["locked", "a", "b", "c", "released"]
    assert [r["status"] for r in results] == ["COMPACTED", "FAILED", "COMPACTED"]
    assert "boom" in results[1]["error"]


def test_a_vacuum_window_shorter_than_a_week_is_refused():
    with pytest.raises(ValueError, match="at least 168"):
        compact_all(None, [], retain_hours=24)


def test_the_compact_command_parses():
    args = cli.build_parser("s", "g").parse_args(["compact", "--dry-run"])

    assert args.command == "compact" and args.dry_run and args.retain_hours == 168

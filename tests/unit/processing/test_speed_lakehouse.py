"""The lakehouse speed layer's paths, SQL and command line, as text: no Spark session needed."""
from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from ecomdata.processing.speed.lakehouse import (  # noqa: E402
    PROGRESS_SCHEMA,
    LakehousePaths,
    build_parser,
    insert_once,
    offer_state_merge,
)
from ecomdata.processing.speed.service import PROGRESS_COLUMNS  # noqa: E402


def test_every_table_and_checkpoint_lives_under_the_root():
    paths = LakehousePaths("s3a://ecomdata-gold/speed/")

    assert paths.observations == "s3a://ecomdata-gold/speed/observations"
    assert paths.offer_state == "s3a://ecomdata-gold/speed/offer_state"
    assert paths.checkpoint("stateful") == "s3a://ecomdata-gold/speed/_checkpoints/stateful"


def test_an_older_state_never_overwrites_a_newer_one():
    sql = offer_state_merge("s3a://g/speed/offer_state", "src")

    assert sql.startswith("MERGE INTO delta.`s3a://g/speed/offer_state` AS t USING src AS s ON t.offer_id = s.offer_id ")
    assert "WHEN MATCHED AND (s.event_time > t.event_time OR " in sql
    assert "s.observation_id > t.observation_id" in sql
    assert sql.endswith("WHEN NOT MATCHED THEN INSERT *")


def test_a_change_is_inserted_once_and_never_rewritten():
    sql = insert_once("u", "src", "change_id")

    assert "ON t.`change_id` = s.`change_id`" in sql
    assert "WHEN MATCHED" not in sql


def test_the_progress_table_has_every_progress_column_in_order():
    names = [part.strip().split(" ")[0] for part in PROGRESS_SCHEMA.split(",")]

    assert names == list(PROGRESS_COLUMNS)


def test_the_command_line_defaults_to_an_always_on_stream_over_the_configured_lake():
    args = build_parser("s3a://gold/speed", "s3a://bronze/marketplace/records").parse_args([])

    assert (args.root, args.records_uri) == ("s3a://gold/speed", "s3a://bronze/marketplace/records")
    assert not args.available_now and not args.auto_loader
    assert args.run_minutes == 0

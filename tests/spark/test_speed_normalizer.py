"""The speed layer's normalisation: records in, canonical ``marketplace-observation.v1`` events out."""
from __future__ import annotations

import json

import pytest

from ecomdata.domain.wire import marketplace_observation_from_wire
from ecomdata.processing.speed.normalizer import normalize_micro_batch
from tests.spark.lake_support import lines_of
from tests.spark.test_normalize import lazada_records, tiki_records
from tests.spark.test_silver_stage import pre_elt_events
from tests.spark_support import requires_spark

pytestmark = requires_spark


def lines_frame(spark, lines):
    return spark.createDataFrame([(line,) for line in lines], "value string")


@pytest.fixture(scope="module")
def tiki(spark):
    events, rejected = normalize_micro_batch(lines_frame(spark, lines_of(tiki_records())))
    return [r.asDict() for r in events.collect()], [r.asDict() for r in rejected.collect()]


def test_every_event_is_a_valid_strict_observation_wire_message(tiki):
    events, _ = tiki

    decoded = [marketplace_observation_from_wire(json.loads(e["value"])) for e in events]

    assert len(decoded) == 4  # the sponsored row is not an observation of an offer's price
    assert {e["key"] for e in events} == {d.partition_key for d in decoded}


def test_the_events_equal_those_the_pre_elt_adapter_produced(tiki):
    events, _ = tiki
    new = {
        d.payload.observation.observation_id: d
        for d in (marketplace_observation_from_wire(json.loads(e["value"])) for e in events)
    }

    old = {e.payload.observation.observation_id: e for e in pre_elt_events()}

    assert set(new) == set(old)
    for observation_id, event in old.items():
        got, want = new[observation_id].payload, event.payload
        assert got.offer == want.offer
        for name in ("current_price", "list_price", "discount_amount", "rating_value", "review_count", "sold_count", "availability"):
            assert getattr(got.observation, name) == getattr(want.observation, name), name


def test_lazada_rows_become_events_too(spark):
    events, _ = normalize_micro_batch(lines_frame(spark, lines_of(lazada_records())))

    decoded = [marketplace_observation_from_wire(json.loads(r.value)) for r in events.collect()]

    assert decoded and {d.marketplace for d in decoded} == {"lazada"}


def test_a_row_that_cannot_be_an_observation_goes_to_the_dlq_with_its_reason(spark):
    broken = [{"name": "no id", "price": 10}, {"id": 4001, "name": "neg", "price": -1}]

    events, rejected = normalize_micro_batch(lines_frame(spark, lines_of(tiki_records(extra_rows=broken))))

    assert events.count() == 4
    assert sorted(r.reason for r in rejected.collect()) == ["missing_listing_id", "missing_or_invalid_price"]

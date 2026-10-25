"""The ``marketplace-record.v1`` envelope: identity, lineage, PII drop, wire."""
from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from ecomdata.domain.acquisition import ListingPageRequest
from ecomdata.domain.observations import ResourceType, create_raw_artifact
from ecomdata.domain.records import (
    RECORD_SCHEMA_VERSION,
    RecordKind,
    RecordWireError,
    SourceRecord,
    build_records,
    drop_fields,
    make_record_id,
    record_from_wire,
    record_json,
    record_to_wire,
)
from ecomdata.domain.sources import LAZADA, TIKI

FETCHED = datetime(2026, 10, 25, 8, 30, tzinfo=UTC)


def artifact(code="tiki", marketplace_id="marketplace-tiki", adapter_version=None):
    return create_raw_artifact(
        marketplace_code=code,
        crawl_run_id="run-1",
        marketplace_id=marketplace_id,
        request_url="https://tiki.vn/laptop/c8095",
        resource_type=ResourceType.LISTING_PAGE,
        fetched_at=FETCHED,
        http_status=200,
        content_type="text/html",
        body_sha256="c" * 64,
        raw_uri="file:///lake/part-0.bin.gz",
        adapter_version=adapter_version or TIKI.adapter_version,
        raw_bytes=123,
    )


ROWS = (
    {"id": 1, "name": "A", "price": 100, "nested": {"x": [1, 2, {"y": None}]}, "advertisement": {"slot": 1}},
    {"id": 2, "name": "B", "price": 200},
)


def records(spec=TIKI, rows=ROWS, context=None, code="tiki", marketplace_id="marketplace-tiki"):
    request = ListingPageRequest(code, marketplace_id, "laptop/c8095", 1)
    return build_records(
        spec,
        request=request,
        raw_artifact=artifact(code, marketplace_id, spec.adapter_version),
        rows=rows,
        page_context={"h1": "Laptop"} if context is None else context,
    )


def test_a_page_yields_its_context_then_one_record_per_row_in_display_order():
    built = records()

    assert [(r.record_kind, r.record_index) for r in built] == [
        (RecordKind.PAGE_CONTEXT, 0),
        (RecordKind.LISTING_ROW, 0),
        (RecordKind.LISTING_ROW, 1),
    ]
    assert [r.payload["id"] for r in built[1:]] == [1, 2]


def test_the_payload_is_the_source_object_unchanged_advertisements_included():
    built = records()

    assert built[1].payload == ROWS[0]
    assert built[2].payload == ROWS[1]
    assert built[0].payload == {"h1": "Laptop"}
    assert built[1].dropped_fields == ()


def test_record_ids_are_deterministic_and_distinct_per_kind_and_index():
    first, second = records(), records()

    assert [r.record_id for r in first] == [r.record_id for r in second]
    assert len({r.record_id for r in first}) == 3
    assert first[1].record_id == make_record_id(first[1].raw_artifact_id, RecordKind.LISTING_ROW, 0)


def test_every_record_carries_the_lineage_of_its_artifact():
    built = records()[1]
    raw = artifact()

    assert built.schema_version == RECORD_SCHEMA_VERSION == "marketplace-record.v1"
    assert (built.source_code, built.marketplace_id) == ("tiki", "marketplace-tiki")
    assert built.raw_artifact_id == raw.raw_artifact_id and built.raw_uri == raw.raw_uri
    assert built.body_sha256 == raw.body_sha256 and built.crawl_run_id == "run-1"
    assert built.adapter_version == TIKI.adapter_version and built.extractor_version == TIKI.extractor_version
    assert (built.request_target, built.request_page) == ("laptop/c8095", 1)
    assert built.fetched_at == FETCHED and built.http_status == 200


def test_personal_data_is_dropped_from_the_context_and_named():
    context = {"mods": {"filter": {}}, "userId": "12345", "anonUid": "abc", "seoInfo": {"t": 1}}
    request = ListingPageRequest("lazada", "marketplace-lazada", "laptop", 1)
    built = build_records(
        LAZADA,
        request=request,
        raw_artifact=artifact("lazada", "marketplace-lazada", LAZADA.adapter_version),
        rows=({"itemId": "1"},),
        page_context=context,
    )

    assert built[0].payload == {"mods": {"filter": {}}, "seoInfo": {"t": 1}}
    assert built[0].dropped_fields == ("anonUid", "userId")
    assert built[1].dropped_fields == ()


def test_drop_fields_does_not_mutate_its_input():
    original = {"a": 1, "b": 2}

    kept, dropped = drop_fields(original, ["b", "c"])

    assert (kept, dropped) == ({"a": 1}, ("b",)) and original == {"a": 1, "b": 2}


def test_a_record_survives_the_wire_unchanged():
    for record in records():
        again = record_from_wire(json.loads(record_json(record)))

        assert again == record
        assert record_to_wire(again) == record_to_wire(record)


def test_the_canonical_json_is_one_line_with_sorted_keys():
    text = record_json(records()[1])

    assert "\n" not in text and ", " not in text
    assert text == json.dumps(json.loads(text), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def test_a_record_with_a_wrong_id_is_refused():
    wire = record_to_wire(records()[1])
    wire["record_id"] = "rec_" + "0" * 64

    with pytest.raises(RecordWireError, match="record_id"):
        record_from_wire(wire)


def test_unknown_and_missing_wire_fields_are_refused():
    wire = record_to_wire(records()[1])

    with pytest.raises(RecordWireError, match="unknown"):
        record_from_wire({**wire, "extra": 1})
    wire.pop("payload")
    with pytest.raises(RecordWireError, match="missing"):
        record_from_wire(wire)
    with pytest.raises(RecordWireError):
        record_from_wire([])


def test_the_partition_key_keeps_one_category_page_in_one_partition():
    built = records()

    assert {r.partition_key for r in built} == {"tiki|laptop/c8095"}


def test_a_record_needs_a_known_kind_and_an_aware_instant():
    record = records()[1]
    fields = {name: getattr(record, name) for name in record.__dataclass_fields__}

    with pytest.raises(ValueError, match="record_kind"):
        SourceRecord(**{**fields, "record_kind": "listing_row"})
    with pytest.raises(ValueError, match="timezone-aware"):
        SourceRecord(**{**fields, "fetched_at": datetime(2026, 10, 25, 8, 30)})

"""Bronze/records: one gzip JSON-lines file per raw artifact."""
from __future__ import annotations

import gzip
from datetime import UTC, datetime

import pytest

from ecomdata.domain.acquisition import ListingPageRequest, RawPersistenceError
from ecomdata.domain.observations import ResourceType, create_raw_artifact
from ecomdata.domain.records import build_records
from ecomdata.domain.sources import TIKI
from ecomdata.ingestion.record_store import decode_records, encode_records, persist_records, read_records, records_path

FETCHED = datetime(2026, 10, 25, 8, 30, tzinfo=UTC)


def artifact(sha):
    return create_raw_artifact(
        marketplace_code="tiki",
        crawl_run_id="run-1",
        marketplace_id="marketplace-tiki",
        request_url="https://tiki.vn/laptop/c8095",
        resource_type=ResourceType.LISTING_PAGE,
        fetched_at=FETCHED,
        http_status=200,
        content_type="text/html",
        body_sha256=sha * 64,
        raw_uri="file:///lake/part-0.bin.gz",
        adapter_version=TIKI.adapter_version,
        raw_bytes=5,
    )


def make_records(rows=({"id": 1, "name": "Điện thoại"}, {"id": 2}), sha="e"):
    request = ListingPageRequest("tiki", "marketplace-tiki", "laptop/c8095", 1)
    return build_records(TIKI, request=request, raw_artifact=artifact(sha), rows=rows, page_context={"h1": "L"})


def test_the_path_partitions_by_source_date_and_hour_and_names_the_artifact():
    path = records_path("tiki", FETCHED, "raw_abc")

    assert path == "marketplace/records/source=tiki/observed_date=2026-10-25/hour=08/raw_artifact_id=raw_abc.jsonl.gz"


def test_records_round_trip_through_the_file_format():
    records = make_records()

    assert decode_records(encode_records(records)) == records


def test_the_file_is_gzip_json_lines_with_one_record_per_line():
    lines = gzip.decompress(encode_records(make_records())).decode("utf-8").splitlines()

    assert len(lines) == 3 and all(line.startswith("{") for line in lines)
    assert "Điện thoại" in lines[1]  # not escaped to \u sequences


def test_the_same_records_always_give_the_same_bytes():
    assert encode_records(make_records()) == encode_records(make_records())


def test_persisting_writes_one_file_to_bronze_and_it_reads_back():
    store = {}

    def write(zone, path, data):
        store[(zone, path)] = data
        return f"file:///lake/{path}"

    records = make_records()
    uri = persist_records(records, writer=write)

    ((zone, path),) = store
    assert zone == "bronze" and uri == f"file:///lake/{path}"
    assert path == records_path("tiki", FETCHED, records[0].raw_artifact_id)
    assert read_records(path, reader=lambda z, p: store.get((z, p))) == records
    assert read_records("missing", reader=lambda z, p: None) is None


def test_a_write_failure_is_a_persistence_error_at_the_records_stage():
    def write(zone, path, data):
        raise OSError("disk full")

    with pytest.raises(RawPersistenceError, match="disk full") as failed:
        persist_records(make_records(), writer=write)

    assert failed.value.write_stage == "RECORDS"


def test_an_empty_or_mixed_file_is_refused():
    with pytest.raises(ValueError, match="at least"):
        persist_records((), writer=lambda *a: "file:///x")
    mixed = make_records()[:1] + make_records(rows=({"id": 3},), sha="f")[1:]
    with pytest.raises(ValueError, match="one raw artifact"):
        persist_records(mixed, writer=lambda *a: "file:///x")

"""Reparse: read a raw artifact back, re-extract it, compare it with Bronze/records."""
from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from ecomdata.domain.acquisition import ExtractedPage, FetchBundle, FetchPart, ListingPageParseError, ListingPageRequest
from ecomdata.domain.records import record_json
from ecomdata.domain.sources import TIKI
from ecomdata.ingestion import reparse
from ecomdata.ingestion.pipeline import acquire_listing_page
from ecomdata.ingestion.raw_store import RawArtifactRef
from ecomdata.ingestion.record_store import encode_records, records_path
from ecomdata.ingestion.sources.tiki.extractor import TikiExtractor
from tests.paths import FIXTURES

URL = "https://tiki.vn/dien-thoai-may-tinh-bang/c1789"
START = datetime(2026, 10, 25, 8, 30, tzinfo=UTC)
PAGE = (FIXTURES / "sources" / "tiki" / "category_page.html").read_bytes()


class Lake:
    def __init__(self):
        self.objects = {}

    def write(self, zone, path, data):
        self.objects[(zone, path)] = data
        return f"file:///lake/{path}"

    def read(self, zone, path):
        return self.objects.get((zone, path))


class StaticFetcher:
    def __init__(self, body=PAGE):
        self.body = body

    def request_url(self, request):
        return URL

    def allowed(self, url):
        return True

    def throttle(self, url):
        pass

    def fetch(self, request, url):
        return FetchBundle(url, START, (FetchPart(url, 200, "text/html", self.body),))


def crawled(body=PAGE):
    """A page acquired for real into an in-memory lake, the way the worker does it."""
    lake = Lake()
    request = ListingPageRequest("tiki", "marketplace-tiki", "dien-thoai-may-tinh-bang/c1789", 1)
    report = acquire_listing_page(
        TIKI, StaticFetcher(body), TikiExtractor(), request, crawl_run_id="run-1", writer=lake.write, clock=lambda: START
    )
    assert report.is_task_success, report.failure
    return lake, report, RawArtifactRef.from_uri(report.raw_artifact.raw_uri)


def run(lake, ref, extractor=None):
    return reparse.reparse_raw_artifact(ref, spec=TIKI, extractor=extractor or TikiExtractor(), reader=lake.read)


def test_the_reference_points_at_what_the_pipeline_actually_wrote():
    lake, report, ref = crawled()

    assert ("bronze", ref.metadata_path) in lake.objects and ("bronze", ref.part_path(0)) in lake.objects
    assert ref.raw_artifact_id == report.raw_artifact.raw_artifact_id


def test_reparsing_a_stored_page_is_identical_to_what_bronze_holds():
    lake, report, ref = crawled()

    outcome = run(lake, ref)

    assert outcome.ok and outcome.status == reparse.IDENTICAL
    assert sorted(outcome.record_ids) == sorted(r.record_id for r in report.records)
    assert len(outcome.record_ids) == 6  # five rows and the page context


def test_reparsing_writes_nothing():
    lake, _, ref = crawled()
    before = dict(lake.objects)

    run(lake, ref)

    assert lake.objects == before


def test_a_checksum_mismatch_stops_before_the_extractor_is_called():
    lake, _, ref = crawled()
    lake.objects[("bronze", ref.part_path(0))] = b"not gzip at all"
    called = []

    class Spy:
        def extract(self, bundle):
            called.append(bundle)

    outcome = run(lake, ref, Spy())

    assert outcome.status in (reparse.CHECKSUM_MISMATCH, reparse.PARSE_FAILED) and not outcome.ok
    assert called == []


def test_a_tampered_part_is_a_checksum_mismatch():
    import gzip

    lake, _, ref = crawled()
    lake.objects[("bronze", ref.part_path(0))] = gzip.compress(b"tampered")

    assert run(lake, ref).status == reparse.CHECKSUM_MISMATCH


def test_an_unknown_metadata_schema_version_is_rejected():
    lake, _, ref = crawled()
    metadata = json.loads(lake.objects[("bronze", ref.metadata_path)])
    metadata["schema_version"] = "marketplace-raw-artifact-metadata.v9"
    lake.objects[("bronze", ref.metadata_path)] = json.dumps(metadata).encode()

    outcome = run(lake, ref)

    assert outcome.status == reparse.PARSE_FAILED and "unsupported" in outcome.error_message


def test_an_extractor_version_change_is_reported_not_reparsed():
    lake, _, ref = crawled()
    newer = TIKI.__class__(**{**TIKI.__dict__, "extractor_version": "2"})

    outcome = reparse.reparse_raw_artifact(ref, spec=newer, extractor=TikiExtractor(), reader=lake.read)

    assert outcome.status == reparse.ADAPTER_VERSION_CHANGED
    assert "tiki-http-extractor-v1" in outcome.error_message and "tiki-http-extractor-v2" in outcome.error_message


def test_changed_extraction_under_the_same_artifact_is_reported_as_diverged():
    lake, _, ref = crawled()

    class Drifted(TikiExtractor):
        def extract(self, bundle):
            page = super().extract(bundle)
            rows = list(page.rows)
            rows[1] = {**rows[1], "price": 1}
            return ExtractedPage(rows=tuple(rows), page_context=page.page_context, last_page=page.last_page)

    outcome = run(lake, ref, Drifted())

    assert outcome.status == reparse.DIVERGED and not outcome.ok
    assert len(outcome.diverged_record_ids) == 1


def test_records_missing_from_bronze_are_reported_not_written():
    lake, report, ref = crawled()
    del lake.objects[("bronze", records_path("tiki", START, report.raw_artifact.raw_artifact_id))]

    outcome = run(lake, ref)

    assert outcome.status == reparse.RECORDS_MISSING
    assert not any(path.endswith(".jsonl.gz") for _, path in lake.objects)


def test_a_record_that_exists_only_in_bronze_is_a_divergence():
    lake, report, ref = crawled()
    path = ("bronze", records_path("tiki", START, report.raw_artifact.raw_artifact_id))
    from ecomdata.ingestion.record_store import decode_records

    kept = decode_records(lake.objects[path])[:-1]
    lake.objects[path] = encode_records(kept)

    outcome = run(lake, ref)

    assert outcome.status == reparse.DIVERGED and len(outcome.diverged_record_ids) == 1


def test_an_extractor_failure_is_reported_rather_than_raised():
    lake, _, ref = crawled()

    class Broken:
        def extract(self, bundle):
            raise ListingPageParseError("layout changed")

    outcome = run(lake, ref, Broken())

    assert outcome.status == reparse.PARSE_FAILED and outcome.error_type == "ListingPageParseError"


def test_reparsing_twice_gives_identical_hashes():
    lake, _, ref = crawled()

    assert run(lake, ref).content_hashes == run(lake, ref).content_hashes


def test_the_diff_returns_sorted_divergences_and_nothing_on_a_match():
    assert reparse.diff_records({"a": "1", "b": "2"}, {"a": "1", "b": "2"}) == ()
    assert reparse.diff_records({"b": "2", "a": "1", "c": "3"}, {"a": "9", "c": "3", "d": "4"}) == ("a", "b", "d")


def test_the_content_hash_covers_the_envelope_and_the_payload():
    lake, report, _ = crawled()
    record = report.records[1]
    other = record.__class__(**{**{n: getattr(record, n) for n in record.__dataclass_fields__}, "payload": {"id": 0}})

    assert reparse.content_hash(record) != reparse.content_hash(other)
    assert reparse.content_hash(record) == reparse.content_hash(record)
    assert json.loads(record_json(record))["payload"] == dict(record.payload)


def test_a_batch_grades_every_artifact():
    lake, _, ref = crawled()

    outcomes = reparse.reparse_batch(
        [ref], spec_for=lambda code: TIKI, extractor_for=lambda code: TikiExtractor(), reader=lake.read
    )

    assert [o.status for o in outcomes] == [reparse.IDENTICAL]


def test_the_report_counts_statuses_and_names_only_identifiers():
    ok = reparse.ReparseOutcome("raw_1", reparse.IDENTICAL)
    bad = reparse.ReparseOutcome(
        "raw_2", reparse.DIVERGED, diverged_record_ids=("rec_1",), error_type="ContentDiverged", error_message="secret"
    )

    report = reparse.summarise([ok, bad], reported_at=START)

    assert report["counts"] == {"DIVERGED": 1, "IDENTICAL": 1} and report["artifacts"] == 2
    assert report["problems"] == [
        {"raw_artifact_id": "raw_2", "status": "DIVERGED", "record_ids": ["rec_1"], "error_type": "ContentDiverged"}
    ]
    assert "secret" not in json.dumps(report)


def cli_args(ref):
    return [
        "--marketplace", ref.marketplace_code, "--observed-date", ref.observed_date, "--hour", ref.hour,
        "--crawl-run-id", ref.crawl_run_id, "--raw-artifact-id", ref.raw_artifact_id,
    ]


def test_the_cli_exits_zero_and_reports_when_every_artifact_is_identical(capsys, tmp_path):
    lake, _, ref = crawled()
    report_path = tmp_path / "report.json"

    code = reparse.main([*cli_args(ref), "--report-path", str(report_path)], reader=lake.read)

    printed = json.loads(capsys.readouterr().out)
    assert code == 0 and printed["counts"] == {"IDENTICAL": 1}
    assert json.loads(report_path.read_text(encoding="utf-8")) == printed


def test_the_cli_exits_non_zero_when_an_artifact_is_not_identical(capsys):
    lake, report, ref = crawled()
    del lake.objects[("bronze", records_path("tiki", START, report.raw_artifact.raw_artifact_id))]

    code = reparse.main(cli_args(ref), reader=lake.read)

    assert code == 1 and json.loads(capsys.readouterr().out)["counts"] == {"RECORDS_MISSING": 1}


def test_a_raw_uri_with_an_escaped_slash_comes_back_unescaped():
    ref = RawArtifactRef("tiki", "2026-10-25", "08", "run/one", "raw_1")
    uri = f"file:///lake/{ref.part_path(0)}"

    assert RawArtifactRef.from_uri(uri) == ref


def test_a_raw_uri_missing_a_component_is_an_error_not_a_guess():
    with pytest.raises(ValueError, match="missing"):
        RawArtifactRef.from_uri("file:///lake/marketplace/raw/marketplace=tiki/part-0.bin.gz")

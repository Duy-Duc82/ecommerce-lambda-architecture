"""Raw reparse tests, Phase 7 plan section 17 items 42-50.

The stored artifact is written by the production ``persist_fetch_result`` into
a dict standing in for Bronze, so the sidecar under test has exactly the shape
the crawler writes. A hand-written fixture would drift away from it silently.
"""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from config.marketplace_schema import (
    ResourceType,
    create_marketplace_offer,
    create_observation_event,
    create_offer_observation,
)
from crawler.contracts import FetchResult, ListingPageRequest
from crawler.raw_store import persist_fetch_result
from crawler.reparse import (
    ADAPTER_VERSION_CHANGED,
    CHECKSUM_MISMATCH,
    DIVERGED,
    IDENTICAL,
    NEW_OBSERVATIONS,
    PARSE_FAILED,
    RawArtifactRef,
    ReparseOutcome,
    classify,
    content_hash,
    diff_reparsed_observations,
    load_raw_artifact,
    main,
    reparse_batch,
    reparse_raw_artifact,
    summarise,
)

FETCHED_AT = datetime(2026, 9, 20, 8, 30, tzinfo=timezone.utc)
CRAWL_RUN = "run-2026-09-20"
BODY = json.dumps(
    {"rows": [
        {"listing_id": "L1", "title": "First product", "price": "100.00"},
        {"listing_id": "L2", "title": "Second product", "price": "250.50"},
    ]},
    sort_keys=True,
).encode("utf-8")


class FakeStore:
    def __init__(self):
        self.objects = {}

    def write(self, zone, path, data):
        self.objects[(zone, path.strip("/"))] = data
        return f"s3a://{zone}/{path.strip('/')}"

    def read(self, zone, path):
        return self.objects.get((zone, path.strip("/")))


class FakeAdapter:
    """Builds real canonical observations from a toy body.

    Using the production factories keeps the observation identity rules — and
    therefore the determinism claim — honest, without tying this test to any
    one marketplace's HTML.
    """

    site_name = "tiki"
    marketplace_id = "marketplace-tiki"
    adapter_version = "fake-v1"

    def __init__(self, *, explode=False, mutate_title=False):
        self.calls = 0
        self.explode = explode
        self.mutate_title = mutate_title

    def parse_listing_page(self, *, request, fetch_result, raw_artifact, crawl_run_id, produced_at):
        self.calls += 1
        if self.explode:
            raise ValueError("the stored body is not the shape this adapter expects")
        rows = json.loads(fetch_result.body.decode("utf-8"))["rows"]
        observations = []
        for index, row in enumerate(rows):
            title = row["title"] + (" (v2)" if self.mutate_title else "")
            offer = create_marketplace_offer(
                marketplace_code=self.site_name,
                marketplace_id=self.marketplace_id,
                platform_listing_id=row["listing_id"],
                seller_id=None,
                product_title=title,
                brand=None,
                category_path=request.target,
                source_url=f"https://tiki.vn/{row['listing_id']}.html",
                currency="VND",
                first_seen_at=raw_artifact.fetched_at,
                last_seen_at=raw_artifact.fetched_at,
            )
            observation = create_offer_observation(
                marketplace_code=self.site_name,
                platform_listing_id=row["listing_id"],
                offer_id=offer.offer_id,
                observed_at=raw_artifact.fetched_at,
                fetched_at=raw_artifact.fetched_at,
                current_price=Decimal(row["price"]),
                raw_uri=raw_artifact.raw_uri,
                raw_sha256=raw_artifact.body_sha256,
                adapter_version=raw_artifact.adapter_version,
                crawl_run_id=crawl_run_id,
                ranking_position=index + 1,
            )
            observations.append(
                create_observation_event(
                    marketplace_code=self.site_name,
                    offer=offer,
                    observation=observation,
                    platform_listing_id=row["listing_id"],
                    produced_at=produced_at,
                )
            )
        from crawler.contracts import ParsedListingPage

        return ParsedListingPage(
            observations=tuple(observations), rejections=(), source_record_count=len(rows),
            duplicate_count=0, last_page=None,
        )


def stored(*, body=BODY, adapter_version="fake-v1"):
    """Write one artifact the way the crawler does and return its reference."""
    store = FakeStore()
    request = ListingPageRequest(
        marketplace_code="tiki", marketplace_id="marketplace-tiki", target="1846", page=1,
        resource_type=ResourceType.LISTING_PAGE,
    )
    fetch_result = FetchResult(
        request_url="https://tiki.vn/api/listing?category=1846&page=1",
        fetched_at=FETCHED_AT, http_status=200, content_type="application/json", body=body,
        elapsed_ms=120,
    )
    persisted = persist_fetch_result(
        request=request, fetch_result=fetch_result, crawl_run_id=CRAWL_RUN,
        adapter_version=adapter_version, writer=store.write,
    )
    ref = RawArtifactRef(
        marketplace_code="tiki",
        observed_date=FETCHED_AT.strftime("%Y-%m-%d"),
        hour=FETCHED_AT.strftime("%H"),
        crawl_run_id=CRAWL_RUN,
        raw_artifact_id=persisted.artifact.raw_artifact_id,
    )
    return store, ref


def clock(offset_seconds=0):
    return lambda: datetime(2026, 9, 30, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


def test_the_reference_points_at_what_the_crawler_actually_wrote():
    store, ref = stored()

    assert ("bronze", ref.body_path) in store.objects
    assert ("bronze", ref.metadata_path) in store.objects


# 42
def test_a_checksum_mismatch_stops_before_the_adapter_is_called():
    store, ref = stored()
    store.objects[("bronze", ref.body_path)] = b"tampered body"
    adapter = FakeAdapter()

    outcome = reparse_raw_artifact(ref, adapter=adapter, reader=store.read, clock=clock())

    assert outcome.status == CHECKSUM_MISMATCH
    assert adapter.calls == 0
    assert "checksum" in outcome.error_message


# 43
def test_an_unknown_metadata_schema_version_is_rejected():
    store, ref = stored()
    metadata = json.loads(store.objects[("bronze", ref.metadata_path)].decode("utf-8"))
    metadata["schema_version"] = "marketplace-raw-artifact-metadata.v99"
    store.objects[("bronze", ref.metadata_path)] = json.dumps(metadata).encode("utf-8")

    with pytest.raises(Exception, match="unsupported raw metadata schema version"):
        load_raw_artifact(ref, reader=store.read)


# 44
def test_an_adapter_version_change_is_reported_not_parsed():
    store, ref = stored(adapter_version="fake-v1")
    adapter = FakeAdapter()
    adapter.adapter_version = "fake-v2"

    outcome = reparse_raw_artifact(ref, adapter=adapter, reader=store.read, clock=clock())

    assert outcome.status == ADAPTER_VERSION_CHANGED
    assert adapter.calls == 0
    assert "fake-v1" in outcome.error_message


# 45
def test_reparsing_reproduces_the_original_observation_ids():
    store, ref = stored()
    adapter = FakeAdapter()

    first = reparse_raw_artifact(ref, adapter=adapter, reader=store.read, clock=clock())

    assert first.status == IDENTICAL
    assert len(first.observation_ids) == 2
    assert first.observation_ids == tuple(sorted(first.content_hashes))


# 46, 49
def test_reparsing_twice_under_different_clocks_yields_identical_hashes():
    store, ref = stored()

    first = reparse_raw_artifact(ref, adapter=FakeAdapter(), reader=store.read, clock=clock(0))
    second = reparse_raw_artifact(ref, adapter=FakeAdapter(), reader=store.read, clock=clock(86_400))

    assert first.observation_ids == second.observation_ids
    assert first.content_hashes == second.content_hashes


# 46
def test_reparsing_writes_nothing():
    store, ref = stored()
    before = dict(store.objects)

    reparse_raw_artifact(ref, adapter=FakeAdapter(), reader=store.read, clock=clock())

    assert store.objects == before


# 47
def test_the_diff_returns_sorted_divergences_and_nothing_on_a_match():
    reparsed = {"obs-b": "hash-2", "obs-a": "hash-1", "obs-c": "hash-3"}

    assert diff_reparsed_observations(reparsed, dict(reparsed)) == ((), ())
    diverged, unseen = diff_reparsed_observations(reparsed, {"obs-a": "hash-1", "obs-c": "other"})
    assert diverged == ("obs-c",)
    assert unseen == ("obs-b",)


# 47
def test_changed_content_under_the_same_id_is_reported_as_diverged():
    store, ref = stored()
    original = reparse_raw_artifact(ref, adapter=FakeAdapter(), reader=store.read, clock=clock())
    changed = reparse_raw_artifact(ref, adapter=FakeAdapter(mutate_title=True), reader=store.read, clock=clock())

    graded = classify(changed, original.content_hashes)

    assert graded.status == DIVERGED
    assert graded.diverged_observation_ids == original.observation_ids


# 48
def test_an_observation_absent_from_silver_is_reported_not_written():
    store, ref = stored()
    outcome = reparse_raw_artifact(ref, adapter=FakeAdapter(), reader=store.read, clock=clock())
    partial = {outcome.observation_ids[0]: outcome.content_hashes[outcome.observation_ids[0]]}

    graded = classify(outcome, partial)

    assert graded.status == NEW_OBSERVATIONS
    assert graded.diverged_observation_ids == (outcome.observation_ids[1],)


def test_a_parser_failure_is_reported_rather_than_raised():
    store, ref = stored()

    outcome = reparse_raw_artifact(ref, adapter=FakeAdapter(explode=True), reader=store.read, clock=clock())

    assert outcome.status == PARSE_FAILED
    assert outcome.error_type == "ValueError"


def test_a_batch_grades_every_artifact_against_silver():
    store, ref = stored()
    baseline = reparse_raw_artifact(ref, adapter=FakeAdapter(), reader=store.read, clock=clock())

    outcomes = reparse_batch(
        [ref], adapter_for=lambda code: FakeAdapter(), reader=store.read, clock=clock(),
        existing=baseline.content_hashes,
    )

    assert [outcome.status for outcome in outcomes] == [IDENTICAL]


def test_the_report_counts_statuses_and_names_only_identifiers():
    outcomes = (
        ReparseOutcome("artifact-1", IDENTICAL),
        ReparseOutcome("artifact-2", DIVERGED, diverged_observation_ids=("obs-1",), error_type="ContentDiverged"),
    )

    report = summarise(outcomes, reported_at=datetime(2026, 9, 30, tzinfo=timezone.utc))

    assert report["counts"] == {DIVERGED: 1, IDENTICAL: 1}
    assert report["problems"][0]["raw_artifact_id"] == "artifact-2"
    assert "error_message" not in report["problems"][0]


# 50
def test_the_cli_exits_non_zero_when_an_artifact_is_not_identical(monkeypatch, capsys):
    import crawler.reparse as reparse

    monkeypatch.setattr(reparse, "reparse_batch", lambda *a, **k: (ReparseOutcome("artifact-1", DIVERGED),))

    code = main([
        "--marketplace", "tiki", "--observed-date", "2026-09-20", "--hour", "08",
        "--crawl-run-id", CRAWL_RUN, "--raw-artifact-id", "artifact-1",
    ])

    assert code == 1
    assert json.loads(capsys.readouterr().out)["counts"] == {DIVERGED: 1}


# 50
def test_the_cli_exits_zero_when_every_artifact_is_identical(monkeypatch, capsys):
    import crawler.reparse as reparse

    monkeypatch.setattr(reparse, "reparse_batch", lambda *a, **k: (ReparseOutcome("artifact-1", IDENTICAL),))

    code = main([
        "--marketplace", "tiki", "--observed-date", "2026-09-20", "--hour", "08",
        "--crawl-run-id", CRAWL_RUN, "--raw-artifact-id", "artifact-1",
    ])

    assert code == 0
    assert json.loads(capsys.readouterr().out)["counts"] == {IDENTICAL: 1}


def test_the_content_hash_ignores_only_the_envelope_produced_at():
    store, ref = stored()
    outcome = reparse_raw_artifact(ref, adapter=FakeAdapter(), reader=store.read, clock=clock())
    one = outcome.observation_ids[0]

    assert len(outcome.content_hashes[one]) == 64
    assert content_hash({"produced_at": "a", "x": 1}) == content_hash({"produced_at": "b", "x": 1})
    assert content_hash({"produced_at": "a", "x": 1}) != content_hash({"produced_at": "a", "x": 2})

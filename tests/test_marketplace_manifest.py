"""Gold manifest tests, Phase 7 plan section 17 items 32-41.

Object storage is a dict. No MinIO, PostgreSQL, Spark session or network.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from batch_layer.marketplace_manifest import (
    ALREADY_CURRENT,
    BACKFILL_REFUSED,
    CURRENT_POINTER_PATH,
    GOLD_ZONE,
    PROMOTED,
    QUALITY_FAILED,
    build_gold_manifest,
    manifest_chain,
    parse_manifest,
    promote_manifest,
    read_current_manifest,
    run_manifest_path,
    serialize_manifest,
    write_run_manifest,
)
from batch_layer.marketplace_postgres import DATASETS
from batch_layer.marketplace_quality import QualityDecision
from batch_layer.marketplace_warehouse import GoldWriteResult, MarketplaceBatchContext

AS_OF = datetime(2026, 9, 29, tzinfo=timezone.utc)


class FakeStore:
    """A dict standing in for the object store, recording every write."""

    def __init__(self):
        self.objects = {}
        self.writes = []

    def write(self, zone, path, data):
        self.objects[(zone, path)] = data
        self.writes.append((zone, path))
        return f"s3a://{zone}/{path}"

    def read(self, zone, path):
        return self.objects.get((zone, path))


def context(run_id="run-1", *, as_of=AS_OF):
    return MarketplaceBatchContext(run_id, as_of, "file:///silver", "file:///gold")


def decision(run_id="run-1", *, passed=True, failures=0):
    return QualityDecision(
        run_id=run_id, passed=passed, rule_version="quality-rules.v1", evaluated_at=AS_OF,
        mandatory_total=13, mandatory_failures=failures, advisory_failures=0, skipped=0,
    )


def writes(run_id="run-1"):
    return {
        name: GoldWriteResult(name, f"file:///gold/runs/run_id={run_id}/{name}", index * 10)
        for index, name in enumerate(DATASETS)
    }


def manifest(run_id="run-1", *, as_of=AS_OF, passed=True, previous=None):
    return build_gold_manifest(
        writes(run_id), decision(run_id, passed=passed, failures=0 if passed else 2),
        context(run_id, as_of=as_of), previous_run_id=previous,
    )


def instants(document):
    """Every value in the document that reads as an instant."""
    found = []

    def walk(node):
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)
        elif isinstance(node, str):
            try:
                found.append(datetime.fromisoformat(node))
            except ValueError:
                pass

    walk(document)
    return found


# 32
def test_the_manifest_lists_exactly_the_ten_datasets_sorted():
    document = manifest()

    assert [dataset.dataset_name for dataset in document.datasets] == sorted(DATASETS)
    assert len(document.datasets) == 10
    anomaly = next(d for d in document.datasets if d.dataset_name == "price_anomaly_daily")
    assert anomaly.partition_columns == ("marketplace", "observed_date")
    assert anomaly.uri.endswith("/price_anomaly_daily")
    reliability = next(d for d in document.datasets if d.dataset_name == "crawl_reliability_daily")
    assert reliability.partition_columns == ("marketplace", "request_date")
    current = next(d for d in document.datasets if d.dataset_name == "offer_current")
    assert current.partition_columns == ()


# 32
def test_the_manifest_carries_every_rule_version_and_the_quality_verdict():
    document = manifest(passed=False)

    assert set(document.rule_versions) == {"anomaly", "counter", "freshness", "quality"}
    assert document.quality["status"] == "FAIL"
    assert document.quality["mandatory_failures"] == 2
    assert document.quality["mandatory_total"] == 13


# 33
def test_serialisation_is_byte_stable_across_two_identical_builds():
    assert serialize_manifest(manifest()) == serialize_manifest(manifest())


# 33
def test_serialisation_is_canonical():
    payload = serialize_manifest(manifest())
    text = payload.decode("utf-8")

    assert text == json.dumps(json.loads(text), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert not text.endswith("\n")


# 34
def test_a_manifest_round_trips_through_its_bytes():
    document = manifest(previous="run-0")

    assert parse_manifest(serialize_manifest(document)) == document


# 34
def test_an_unknown_schema_version_is_refused():
    payload = json.loads(serialize_manifest(manifest()).decode("utf-8"))
    payload["manifest_schema_version"] = "marketplace-gold-manifest.v99"

    with pytest.raises(ValueError, match="unsupported manifest schema version"):
        parse_manifest(json.dumps(payload).encode("utf-8"))


# 35
def test_a_refused_run_writes_its_manifest_and_leaves_the_pointer_alone():
    store = FakeStore()
    good = manifest("run-1")
    promote_manifest(good, writer=store.write, reader=store.read)
    pointer_before = store.objects[(GOLD_ZONE, CURRENT_POINTER_PATH)]

    result = promote_manifest(manifest("run-2", passed=False), writer=store.write, reader=store.read)

    assert result.promoted is False
    assert result.reason == QUALITY_FAILED
    assert (GOLD_ZONE, run_manifest_path("run-2")) in store.objects
    assert store.objects[(GOLD_ZONE, CURRENT_POINTER_PATH)] == pointer_before


# 36
def test_a_passing_run_writes_a_pointer_identical_to_its_run_manifest():
    store = FakeStore()
    document = manifest("run-1")

    result = promote_manifest(document, writer=store.write, reader=store.read)

    assert result.promoted is True
    assert result.reason == PROMOTED
    assert store.objects[(GOLD_ZONE, CURRENT_POINTER_PATH)] == store.objects[(GOLD_ZONE, run_manifest_path("run-1"))]
    assert read_current_manifest(reader=store.read) == document


# 37
def test_the_manifest_chains_back_to_the_pointer_it_replaced():
    store = FakeStore()
    first = manifest("run-1")
    promote_manifest(first, writer=store.write, reader=store.read)

    previous = read_current_manifest(reader=store.read).run_id
    second = manifest("run-2", as_of=AS_OF + timedelta(days=1), previous=previous)
    promote_manifest(second, writer=store.write, reader=store.read)

    assert second.previous_run_id == "run-1"
    assert read_current_manifest(reader=store.read).previous_run_id == "run-1"
    assert manifest_chain([second, first]) == ("run-2", "run-1")


# 38
def test_promoting_the_run_that_is_already_current_writes_nothing():
    store = FakeStore()
    document = manifest("run-1")
    promote_manifest(document, writer=store.write, reader=store.read)
    written = len(store.writes)

    result = promote_manifest(document, writer=store.write, reader=store.read)

    assert result.reason == ALREADY_CURRENT
    assert result.promoted is False
    # Only the run manifest is rewritten, with identical bytes; the pointer is
    # not touched.
    assert store.writes[written:] == [(GOLD_ZONE, run_manifest_path("run-1"))]


# 39
def test_an_older_as_of_is_refused_unless_backfill_is_allowed():
    store = FakeStore()
    promote_manifest(manifest("run-2", as_of=AS_OF + timedelta(days=2)), writer=store.write, reader=store.read)
    older = manifest("run-1", as_of=AS_OF)

    refused = promote_manifest(older, writer=store.write, reader=store.read)
    assert refused.promoted is False
    assert refused.reason == BACKFILL_REFUSED
    assert read_current_manifest(reader=store.read).run_id == "run-2"

    allowed = promote_manifest(older, writer=store.write, reader=store.read, allow_backfill=True)
    assert allowed.promoted is True
    assert read_current_manifest(reader=store.read).run_id == "run-1"


# 40
def test_the_manifest_carries_no_wall_clock():
    document = manifest()
    payload = json.loads(serialize_manifest(document).decode("utf-8"))

    assert document.created_at == document.as_of
    assert payload["created_at"] == payload["as_of"]
    assert set(instants(payload)) == {AS_OF}


# 41
def test_an_absent_pointer_reads_as_no_current_manifest():
    store = FakeStore()

    assert read_current_manifest(reader=store.read) is None


# 41
def test_a_transport_error_does_not_become_an_implicit_first_promotion():
    def broken_reader(zone, path):
        raise ConnectionError("object store unreachable")

    store = FakeStore()

    with pytest.raises(ConnectionError):
        promote_manifest(manifest(), writer=store.write, reader=broken_reader)
    # The run manifest is written before the pointer is consulted; the pointer
    # itself must not have been touched.
    assert (GOLD_ZONE, CURRENT_POINTER_PATH) not in store.objects


def test_a_decision_from_another_run_cannot_build_a_manifest():
    with pytest.raises(ValueError, match="another run"):
        build_gold_manifest(writes(), decision("run-9"), context("run-1"), previous_run_id=None)


def test_a_run_manifest_path_escapes_the_run_id():
    # The context already restricts run_id to a safe alphabet, so this is the
    # second line of defence: the path builder must not trust that either.
    path = run_manifest_path("run/with slash")

    assert "run%2Fwith%20slash" in path
    assert "run/with slash" not in path


def test_the_run_manifest_lands_under_the_run_scoped_path():
    store = FakeStore()

    uri = write_run_manifest(manifest("run-1"), writer=store.write)

    assert uri.endswith("/manifests/run_id=run-1/manifest.json")
    assert store.writes == [(GOLD_ZONE, run_manifest_path("run-1"))]

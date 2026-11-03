import gzip
import hashlib
import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from ecomdata.domain.acquisition import FetchBundle, FetchPart, ListingPageRequest, RawPersistenceError
from ecomdata.domain.identity import make_raw_artifact_id
from ecomdata.ingestion.raw_store import (
    RAW_METADATA_SCHEMA_VERSION,
    RAW_METADATA_SCHEMA_VERSION_V1,
    RawArtifactRef,
    RawArtifactUnreadable,
    load_raw_artifact,
    manifest_sha256,
    persist_fetch_bundle,
)

FETCHED = datetime(2026, 9, 4, 8, 30, tzinfo=UTC)
BODY = b"<html>\xff\xfe not utf-8 </html>"
XHR = b'{"mods":{"listItems":[]}}'
URL = "https://www.lazada.vn/laptop/?page=2"


def _request():
    return ListingPageRequest("Lazada", "marketplace-lazada", "laptop", 2)


def _bundle(captured=True, fetched_at=FETCHED):
    parts = [FetchPart(URL, 200, "text/html", BODY, elapsed_ms=17)]
    if captured:
        parts.append(FetchPart(URL + "&ajax=true", 200, "application/json", XHR, role="xhr"))
    return FetchBundle(URL, fetched_at, tuple(parts), retry_after="3")


class Store:
    def __init__(self, fail_at=None):
        self.objects, self.order, self.fail_at = {}, [], fail_at

    def write(self, zone, path, data):
        if self.fail_at is not None and len(self.order) + 1 == self.fail_at:
            raise OSError("disk full")
        self.order.append(path)
        self.objects[(zone, path)] = data
        return f"file:///lake/{path}"

    def read(self, zone, path):
        return self.objects.get((zone, path))


def _persist(store=None, bundle=None, **overrides):
    store = store or Store()
    kwargs = {"request": _request(), "bundle": bundle or _bundle(), "crawl_run_id": "run/one",
              "adapter_version": "lazada-browser-extractor-v1", "writer": store.write}
    kwargs.update(overrides)
    return store, persist_fetch_bundle(**kwargs)


def test_every_part_is_written_gzipped_before_the_manifest():
    store, result = _persist()

    assert [p.split("/")[-1] for p in store.order] == ["part-0.bin.gz", "part-1.bin.gz", "metadata.json"]
    assert gzip.decompress(store.objects[("bronze", store.order[0])]) == BODY
    assert gzip.decompress(store.objects[("bronze", store.order[1])]) == XHR
    assert result.artifact.raw_uri == f"file:///lake/{store.order[0]}"
    assert result.artifact.raw_bytes == len(BODY) + len(XHR)
    assert "marketplace/raw/marketplace=lazada/observed_date=2026-09-04/hour=08/crawl_run_id=run%2Fone/" in store.order[0]


def test_the_manifest_lists_each_part_with_its_checksum_and_role():
    store, result = _persist()

    metadata = json.loads(store.objects[("bronze", store.order[-1])])
    assert metadata["schema_version"] == RAW_METADATA_SCHEMA_VERSION == "marketplace-raw-artifact-metadata.v2"
    assert [(p["index"], p["role"], p["encoding"], p["bytes"]) for p in metadata["parts"]] == [
        (0, "document", "gzip", len(BODY)),
        (1, "xhr", "gzip", len(XHR)),
    ]
    assert metadata["parts"][0]["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert metadata["parts"][1]["url"] == URL + "&ajax=true"
    assert metadata["request"] == {"page": 2, "target": "laptop"}
    assert metadata["response"] == {"elapsed_ms": 17, "retry_after": "3"}
    assert metadata["raw_artifact"]["raw_artifact_id"] == result.artifact.raw_artifact_id


def test_the_artifact_checksum_is_the_manifests_so_it_covers_every_part():
    _, with_xhr = _persist(bundle=_bundle(captured=True))
    _, without = _persist(bundle=_bundle(captured=False))

    assert with_xhr.artifact.body_sha256 == manifest_sha256(with_xhr.artifact.parts)
    assert with_xhr.artifact.body_sha256 != without.artifact.body_sha256
    assert with_xhr.artifact.raw_artifact_id != without.artifact.raw_artifact_id


def test_the_artifact_id_follows_the_identity_helper():
    store, result = _persist()

    expected = make_raw_artifact_id("lazada", URL, FETCHED, manifest_sha256(result.artifact.parts))
    assert result.artifact.raw_artifact_id == expected and expected in store.order[0]


def test_identical_inputs_produce_identical_paths_and_bytes():
    first, _ = _persist()
    second, _ = _persist()

    assert first.order == second.order
    assert first.objects == second.objects


def test_a_part_write_failure_stops_before_the_manifest():
    store = Store(fail_at=2)

    with pytest.raises(RawPersistenceError, match="disk full") as failed:
        _persist(store)

    assert failed.value.write_stage == "PART" and failed.value.raw_artifact is None
    assert [p.split("/")[-1] for p in store.order] == ["part-0.bin.gz"]


def test_a_manifest_write_failure_retains_the_artifact():
    store = Store(fail_at=3)

    with pytest.raises(RawPersistenceError, match="disk full") as failed:
        _persist(store)

    assert failed.value.write_stage == "MANIFEST"
    assert failed.value.raw_artifact is not None and failed.value.raw_artifact.raw_uri.endswith("/part-0.bin.gz")
    assert isinstance(failed.value.__cause__, OSError)


def test_the_path_uses_the_utc_date_and_hour_of_an_offset_timestamp():
    store, _ = _persist(bundle=_bundle(fetched_at=datetime(2026, 9, 4, 0, 30, tzinfo=timezone(timedelta(hours=-7)))))

    assert "observed_date=2026-09-04/hour=07/" in store.order[0]


def test_the_body_is_stored_as_opaque_bytes():
    store, result = _persist()

    assert gzip.decompress(store.objects[("bronze", store.order[0])]) == BODY
    assert result.artifact.parts[0].sha256 == hashlib.sha256(BODY).hexdigest()


# --- reading back ---------------------------------------------------------


def test_a_v2_artifact_reads_back_equal_to_what_was_written():
    store, result = _persist()
    ref = RawArtifactRef.from_uri(result.artifact.raw_uri)

    loaded = load_raw_artifact(ref, reader=store.read)

    assert loaded.schema_version == RAW_METADATA_SCHEMA_VERSION
    assert loaded.bundle.document.body == BODY and [p.body for p in loaded.bundle.captured] == [XHR]
    assert loaded.artifact == result.artifact
    assert loaded.request == _request().__class__("lazada", "marketplace-lazada", "laptop", 2)
    assert loaded.bundle.retry_after == "3"


def test_a_corrupted_part_is_never_handed_back():
    store, result = _persist()
    ref = RawArtifactRef.from_uri(result.artifact.raw_uri)
    store.objects[("bronze", ref.part_path(1))] = gzip.compress(b"tampered")

    with pytest.raises(RawArtifactUnreadable, match="checksum"):
        load_raw_artifact(ref, reader=store.read)


def test_a_missing_part_or_manifest_is_unreadable():
    store, result = _persist()
    ref = RawArtifactRef.from_uri(result.artifact.raw_uri)
    del store.objects[("bronze", ref.part_path(0))]
    with pytest.raises(RawArtifactUnreadable, match="no stored part"):
        load_raw_artifact(ref, reader=store.read)
    with pytest.raises(RawArtifactUnreadable, match="no metadata"):
        load_raw_artifact(ref, reader=lambda zone, path: None)


def _v1_store(body=BODY):
    """An artifact as the pre-refactor crawler wrote it: one body.bin and a v1 sidecar."""
    sha = hashlib.sha256(body).hexdigest()
    artifact_id = make_raw_artifact_id("tiki", "https://tiki.vn/laptop/c8095", FETCHED, sha)
    ref = RawArtifactRef.for_fetch("tiki", FETCHED, "run-1", artifact_id)
    metadata = {
        "schema_version": RAW_METADATA_SCHEMA_VERSION_V1,
        "raw_artifact": {
            "raw_artifact_id": artifact_id,
            "crawl_run_id": "run-1",
            "marketplace_id": "marketplace-tiki",
            "request_url": "https://tiki.vn/laptop/c8095",
            "resource_type": "LISTING_PAGE",
            "fetched_at": FETCHED.isoformat(),
            "http_status": 200,
            "content_type": "text/html",
            "body_sha256": sha,
            "raw_uri": f"file:///lake/{ref.body_path}",
            "adapter_version": "tiki-category-html-v1",
            "raw_bytes": len(body),
        },
        "request": {"target": "laptop/c8095", "page": 1},
        "response": {"retry_after": None, "elapsed_ms": 5},
    }
    store = Store()
    store.objects[("bronze", ref.body_path)] = body
    store.objects[("bronze", ref.metadata_path)] = json.dumps(metadata).encode()
    return store, ref


def test_a_v1_artifact_is_still_readable():
    store, ref = _v1_store()

    loaded = load_raw_artifact(ref, reader=store.read)

    assert loaded.schema_version == RAW_METADATA_SCHEMA_VERSION_V1
    assert loaded.bundle.document.body == BODY and loaded.bundle.captured == ()
    assert loaded.artifact.adapter_version == "tiki-category-html-v1" and loaded.artifact.parts == ()
    assert loaded.request.target == "laptop/c8095"


def test_a_corrupted_v1_body_is_refused():
    store, ref = _v1_store()
    store.objects[("bronze", ref.body_path)] = b"tampered"

    with pytest.raises(RawArtifactUnreadable, match="checksum"):
        load_raw_artifact(ref, reader=store.read)


def test_an_unknown_schema_version_is_refused():
    store, ref = _v1_store()
    metadata = json.loads(store.objects[("bronze", ref.metadata_path)])
    metadata["schema_version"] = "marketplace-raw-artifact-metadata.v9"
    store.objects[("bronze", ref.metadata_path)] = json.dumps(metadata).encode()

    with pytest.raises(RawArtifactUnreadable, match="unsupported"):
        load_raw_artifact(ref, reader=store.read)


def test_a_reference_is_read_back_from_a_stored_uri_and_a_partial_one_is_refused():
    _, result = _persist()

    ref = RawArtifactRef.from_uri(result.artifact.raw_uri)

    assert (ref.marketplace_code, ref.observed_date, ref.hour, ref.crawl_run_id) == ("lazada", "2026-09-04", "08", "run/one")
    assert ref.raw_artifact_id == result.artifact.raw_artifact_id
    with pytest.raises(ValueError, match="missing"):
        RawArtifactRef.from_uri("file:///lake/marketplace/raw/marketplace=lazada/part-0.bin.gz")

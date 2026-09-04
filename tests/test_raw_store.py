import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from common.identity import make_raw_artifact_id
from crawler.contracts import FetchResult, ListingPageRequest, RawPersistenceError
from crawler.raw_store import RAW_METADATA_SCHEMA_VERSION, persist_fetch_result


UTC = timezone.utc
FETCHED = datetime(2026, 9, 4, 8, 30, tzinfo=UTC)
BODY = b"{\n  \"data\": [\xff]\n}"


def _request():
    return ListingPageRequest("Tiki", "marketplace-tiki", "1846/phones", 2)


def _fetch():
    return FetchResult(
        "https://tiki.vn/api/listings?category=1846%2Fphones&page=2&limit=40",
        FETCHED,
        200,
        "application/json",
        BODY,
        retry_after="3",
        elapsed_ms=17,
    )


def test_persists_exact_body_then_deterministic_metadata():
    calls = []

    def writer(zone, path, data):
        calls.append((zone, path, data))
        return f"file:///lake/{path}"

    result = persist_fetch_result(
        request=_request(),
        fetch_result=_fetch(),
        crawl_run_id="run/one",
        adapter_version="tiki-listing-v1",
        writer=writer,
    )

    assert [call[1].split("/")[-1] for call in calls] == ["body.bin", "metadata.json"]
    assert calls[0][0] == calls[1][0] == "bronze"
    assert calls[0][2] == BODY
    assert result.artifact.raw_uri == f"file:///lake/{calls[0][1]}"
    assert result.artifact.body_sha256 == hashlib.sha256(BODY).hexdigest()
    assert result.artifact.raw_bytes == len(BODY)
    assert "marketplace/raw/marketplace=tiki/" in calls[0][1]
    assert "observed_date=2026-09-04/hour=08/" in calls[0][1]
    assert "crawl_run_id=run%2Fone/" in calls[0][1]
    assert calls[0][1].endswith("/body.bin")
    assert calls[1][1].endswith("/metadata.json")

    metadata = json.loads(calls[1][2].decode("utf-8"))
    assert metadata["schema_version"] == RAW_METADATA_SCHEMA_VERSION
    assert metadata["raw_artifact"]["raw_artifact_id"] == result.artifact.raw_artifact_id
    assert metadata["request"] == {"page": 2, "target": "1846/phones"}
    assert metadata["response"] == {"elapsed_ms": 17, "retry_after": "3"}


def test_raw_artifact_id_uses_phase1_identity_helper():
    calls = []
    result = persist_fetch_result(
        request=_request(),
        fetch_result=_fetch(),
        crawl_run_id="run-1",
        adapter_version="tiki-listing-v1",
        writer=lambda zone, path, data: calls.append((zone, path, data)) or f"file:///tmp/{path}",
    )
    expected = make_raw_artifact_id(
        "tiki",
        _fetch().request_url,
        FETCHED,
        hashlib.sha256(BODY).hexdigest(),
    )
    assert result.artifact.raw_artifact_id == expected
    assert expected in calls[0][1]


def test_identical_inputs_produce_identical_paths_and_metadata_bytes():
    first = []
    second = []
    kwargs = {
        "request": _request(),
        "fetch_result": _fetch(),
        "crawl_run_id": "run-1",
        "adapter_version": "tiki-listing-v1",
    }
    persist_fetch_result(**kwargs, writer=lambda z, p, d: first.append((z, p, d)) or f"file:///x/{p}")
    persist_fetch_result(**kwargs, writer=lambda z, p, d: second.append((z, p, d)) or f"file:///x/{p}")
    assert [(z, p) for z, p, _ in first] == [(z, p) for z, p, _ in second]
    assert [data for _, _, data in first] == [data for _, _, data in second]


def test_body_writer_failure_stops_before_metadata():
    calls = []

    def writer(zone, path, data):
        calls.append(path)
        raise OSError("disk full")

    with pytest.raises(RawPersistenceError, match="disk full") as exc_info:
        persist_fetch_result(
            request=_request(),
            fetch_result=_fetch(),
            crawl_run_id="run-1",
            adapter_version="tiki-listing-v1",
            writer=writer,
        )
    assert exc_info.value.write_stage == "BODY"
    assert exc_info.value.raw_artifact is None
    assert len(calls) == 1


def test_metadata_writer_failure_retains_body_artifact():
    calls = []

    def writer(zone, path, data):
        calls.append((zone, path, data))
        if len(calls) == 2:
            raise OSError("metadata unavailable")
        return f"file:///lake/{path}"

    with pytest.raises(RawPersistenceError, match="metadata unavailable") as exc_info:
        persist_fetch_result(
            request=_request(),
            fetch_result=_fetch(),
            crawl_run_id="run-1",
            adapter_version="tiki-listing-v1",
            writer=writer,
        )
    assert len(calls) == 2
    assert exc_info.value.write_stage == "METADATA"
    assert exc_info.value.raw_artifact is not None
    assert exc_info.value.raw_artifact.raw_uri.endswith("/body.bin")
    assert isinstance(exc_info.value.__cause__, OSError)


def test_path_uses_utc_date_and_hour_for_offset_timestamp():
    calls = []
    fetch = FetchResult(
        "https://example.test/page",
        datetime(2026, 9, 4, 0, 30, tzinfo=timezone(timedelta(hours=-7))),
        200,
        None,
        b"{}",
    )
    persist_fetch_result(
        request=ListingPageRequest("test", "marketplace-test", "x", 1),
        fetch_result=fetch,
        crawl_run_id="run-1",
        adapter_version="test-v1",
        writer=lambda z, p, d: calls.append(p) or f"file:///tmp/{p}",
    )
    assert "observed_date=2026-09-04/hour=07/" in calls[0]


def test_function_never_decodes_body():
    calls = []
    result = persist_fetch_result(
        request=_request(),
        fetch_result=_fetch(),
        crawl_run_id="run-1",
        adapter_version="tiki-listing-v1",
        writer=lambda z, p, d: calls.append(d) or f"file:///tmp/{p}",
    )
    assert calls[0] == BODY
    assert result.artifact.raw_bytes == len(BODY)

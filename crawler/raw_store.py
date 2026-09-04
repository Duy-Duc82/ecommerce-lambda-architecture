"""Durable raw-response persistence for the marketplace acquisition boundary."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Callable
from urllib.parse import quote

from common.identity import make_raw_artifact_id
from common.object_store import put_bytes
from common.serialization import serialize_for_wire
from config.marketplace_schema import RawArtifact, create_raw_artifact
from crawler.contracts import (
    FetchResult,
    ListingPageRequest,
    RawPersistenceError,
)


RAW_METADATA_SCHEMA_VERSION = "marketplace-raw-artifact-metadata.v1"


@dataclass(frozen=True)
class PersistedRawArtifact:
    artifact: RawArtifact
    metadata_uri: str


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _component(value: str) -> str:
    return quote(_require_text(value, "path component"), safe="-_.~")


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        serialize_for_wire(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _raw_paths(
    *,
    marketplace_code: str,
    fetched_at: datetime,
    crawl_run_id: str,
    raw_artifact_id: str,
) -> tuple[str, str]:
    observed_date = fetched_at.strftime("%Y-%m-%d")
    hour = fetched_at.strftime("%H")
    base = (
        "marketplace/raw/"
        f"marketplace={_component(marketplace_code)}/"
        f"observed_date={observed_date}/"
        f"hour={hour}/"
        f"crawl_run_id={_component(crawl_run_id)}/"
        f"raw_artifact_id={_component(raw_artifact_id)}"
    )
    return f"{base}/body.bin", f"{base}/metadata.json"


def persist_fetch_result(
    *,
    request: ListingPageRequest,
    fetch_result: FetchResult,
    crawl_run_id: str,
    adapter_version: str,
    writer: Callable[[str, str, bytes], str] = put_bytes,
) -> PersistedRawArtifact:
    """Write one response body and its sidecar, in that order.

    The response body is deliberately treated as opaque bytes.  Parsing is the
    caller's responsibility and is only safe after this function returns.
    """
    if not isinstance(request, ListingPageRequest):
        raise TypeError("request must be a ListingPageRequest")
    if not isinstance(fetch_result, FetchResult):
        raise TypeError("fetch_result must be a FetchResult")
    crawl_run_id = _require_text(crawl_run_id, "crawl_run_id")
    adapter_version = _require_text(adapter_version, "adapter_version")
    if not callable(writer):
        raise TypeError("writer must be callable")

    body_sha256 = hashlib.sha256(fetch_result.body).hexdigest()
    raw_artifact_id = make_raw_artifact_id(
        request.marketplace_code,
        fetch_result.request_url,
        fetch_result.fetched_at,
        body_sha256,
    )
    body_path, metadata_path = _raw_paths(
        marketplace_code=request.marketplace_code,
        fetched_at=fetch_result.fetched_at,
        crawl_run_id=crawl_run_id,
        raw_artifact_id=raw_artifact_id,
    )

    try:
        body_uri = writer("bronze", body_path, fetch_result.body)
        artifact = create_raw_artifact(
            marketplace_code=request.marketplace_code,
            crawl_run_id=crawl_run_id,
            marketplace_id=request.marketplace_id,
            request_url=fetch_result.request_url,
            resource_type=request.resource_type,
            fetched_at=fetch_result.fetched_at,
            http_status=fetch_result.http_status,
            content_type=fetch_result.content_type,
            body_sha256=body_sha256,
            raw_uri=body_uri,
            adapter_version=adapter_version,
            raw_bytes=len(fetch_result.body),
        )
    except Exception as exc:
        raise RawPersistenceError(
            f"could not persist raw body: {exc}",
            write_stage="BODY",
        ) from exc

    metadata = {
        "schema_version": RAW_METADATA_SCHEMA_VERSION,
        "raw_artifact": artifact,
        "request": {
            "target": request.target,
            "page": request.page,
        },
        "response": {
            "retry_after": fetch_result.retry_after,
            "elapsed_ms": fetch_result.elapsed_ms,
        },
    }
    try:
        metadata_uri = writer("bronze", metadata_path, _canonical_json(metadata))
    except Exception as exc:
        raise RawPersistenceError(
            f"could not persist raw metadata: {exc}",
            write_stage="METADATA",
            raw_artifact=artifact,
        ) from exc

    try:
        metadata_uri = _require_text(metadata_uri, "metadata_uri")
    except Exception as exc:
        raise RawPersistenceError(
            f"raw metadata writer returned an invalid URI: {exc}",
            write_stage="METADATA",
            raw_artifact=artifact,
        ) from exc
    return PersistedRawArtifact(artifact=artifact, metadata_uri=metadata_uri)

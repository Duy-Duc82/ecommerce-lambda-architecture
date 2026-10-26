"""Durable raw persistence for the acquisition boundary (Bronze/raw).

A raw artifact is the set of responses one fetch returned, kept as it came:

* v2 (written): ``part-<n>.bin.gz`` per response (the page document first, then
  any captured XHR), and ``metadata.json`` listing them with their URL, status,
  content type, role, sha256 of the uncompressed body, byte count and encoding.
  The artifact's ``body_sha256`` is the sha256 of the parts list, so one hash
  covers every response. ``raw_uri`` names ``part-0.bin.gz``.
* v1 (still readable): one uncompressed ``body.bin`` and a sidecar
  ``metadata.json``.

Parts are written before the manifest, so a manifest never names a missing part.
"""
from __future__ import annotations

import hashlib
import json
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote, unquote

from ecomdata.domain import validation as v
from ecomdata.domain.acquisition import (
    ROLE_DOCUMENT,
    FetchBundle,
    FetchPart,
    ListingPageRequest,
    RawPersistenceError,
)
from ecomdata.domain.identity import make_raw_artifact_id
from ecomdata.domain.observations import RawArtifact, RawPart, ResourceType, create_raw_artifact
from ecomdata.domain.serialization import serialize_for_wire
from ecomdata.infra.object_store import gunzip_bytes, gzip_bytes

RAW_METADATA_SCHEMA_VERSION = "marketplace-raw-artifact-metadata.v2"
RAW_METADATA_SCHEMA_VERSION_V1 = "marketplace-raw-artifact-metadata.v1"
BRONZE_ZONE = "bronze"
RAW_ROOT = "marketplace/raw"

Writer = Callable[[str, str, bytes], str]
Reader = Callable[[str, str], "bytes | None"]


@dataclass(frozen=True)
class PersistedRawArtifact:
    artifact: RawArtifact
    metadata_uri: str


def _component(value: str) -> str:
    return quote(v.require_text(value, "path component"), safe="-_.~")


def canonical_json(value: object) -> bytes:
    return json.dumps(serialize_for_wire(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def part_path(base: str, index: int) -> str:
    return f"{base}/part-{index}.bin.gz"


@dataclass(frozen=True)
class RawArtifactRef:
    """The five path components that address one artifact directory."""

    marketplace_code: str
    observed_date: str
    hour: str
    crawl_run_id: str
    raw_artifact_id: str

    @classmethod
    def for_fetch(cls, marketplace_code: str, fetched_at: datetime, crawl_run_id: str, raw_artifact_id: str):
        return cls(
            marketplace_code, fetched_at.strftime("%Y-%m-%d"), fetched_at.strftime("%H"), crawl_run_id, raw_artifact_id
        )

    @property
    def base(self) -> str:
        return (
            f"{RAW_ROOT}/"
            f"marketplace={_component(self.marketplace_code)}/"
            f"observed_date={self.observed_date}/"
            f"hour={self.hour}/"
            f"crawl_run_id={_component(self.crawl_run_id)}/"
            f"raw_artifact_id={_component(self.raw_artifact_id)}"
        )

    @property
    def metadata_path(self) -> str:
        return f"{self.base}/metadata.json"

    @property
    def body_path(self) -> str:
        """The v1 body, one uncompressed file."""
        return f"{self.base}/body.bin"

    def part_path(self, index: int) -> str:
        return part_path(self.base, index)

    @classmethod
    def from_uri(cls, uri: str) -> RawArtifactRef:
        """The inverse of :attr:`base`, for a ``raw_uri`` out of the audit.

        The audit stores where the first stored response landed, not the five
        components that address the artifact. They are all in the path, so this
        reads them back. A path missing one is an error, never a guess:
        reparsing the wrong artifact would report a divergence that is really a
        lookup bug.
        """
        # Split before unquoting: a component may itself contain an encoded slash.
        parts = {
            key: unquote(value)
            for key, value in (
                segment.split("=", 1) for segment in uri.split("/") if "=" in segment and not segment.startswith("=")
            )
        }
        wanted = ("marketplace", "observed_date", "hour", "crawl_run_id", "raw_artifact_id")
        missing = [name for name in wanted if name not in parts]
        if missing:
            raise ValueError(f"raw_uri is missing {', '.join(missing)}: {uri}")
        return cls(
            parts["marketplace"], parts["observed_date"], parts["hour"], parts["crawl_run_id"], parts["raw_artifact_id"]
        )


def manifest_sha256(parts: tuple[RawPart, ...]) -> str:
    """One hash over every stored response: the sha256 of the canonical parts list."""
    return hashlib.sha256(canonical_json({"parts": list(parts)})).hexdigest()


def persist_fetch_bundle(
    *,
    request: ListingPageRequest,
    bundle: FetchBundle,
    crawl_run_id: str,
    adapter_version: str,
    writer: Writer,
) -> PersistedRawArtifact:
    """Write every response of a bundle, then the manifest, in that order.

    Bodies are opaque bytes; parsing is only safe after this returns.
    """
    if not isinstance(request, ListingPageRequest):
        raise TypeError("request must be a ListingPageRequest")
    if not isinstance(bundle, FetchBundle):
        raise TypeError("bundle must be a FetchBundle")
    crawl_run_id = v.require_text(crawl_run_id, "crawl_run_id")
    adapter_version = v.require_text(adapter_version, "adapter_version")
    if not callable(writer):
        raise TypeError("writer must be callable")

    ordered = (bundle.document, *bundle.captured)
    parts = tuple(
        RawPart(
            index=index,
            url=part.url,
            http_status=part.http_status,
            content_type=part.content_type,
            role=part.role,
            sha256=hashlib.sha256(part.body).hexdigest(),
            bytes=len(part.body),
            encoding="gzip",
        )
        for index, part in enumerate(ordered)
    )
    body_sha256 = manifest_sha256(parts)
    raw_artifact_id = make_raw_artifact_id(request.marketplace_code, bundle.request_url, bundle.fetched_at, body_sha256)
    ref = RawArtifactRef.for_fetch(request.marketplace_code, bundle.fetched_at, crawl_run_id, raw_artifact_id)

    try:
        uris = [writer(BRONZE_ZONE, ref.part_path(index), gzip_bytes(part.body)) for index, part in enumerate(ordered)]
        artifact = create_raw_artifact(
            marketplace_code=request.marketplace_code,
            crawl_run_id=crawl_run_id,
            marketplace_id=request.marketplace_id,
            request_url=bundle.request_url,
            resource_type=request.resource_type,
            fetched_at=bundle.fetched_at,
            http_status=bundle.http_status,
            content_type=bundle.content_type,
            body_sha256=body_sha256,
            raw_uri=uris[0],
            adapter_version=adapter_version,
            raw_bytes=bundle.total_bytes,
            parts=parts,
        )
    except Exception as exc:
        raise RawPersistenceError(f"could not persist a raw part: {exc}", write_stage="PART") from exc

    metadata = {
        "schema_version": RAW_METADATA_SCHEMA_VERSION,
        "raw_artifact": artifact,
        "parts": list(parts),
        "request": {"target": request.target, "page": request.page},
        "response": {
            "retry_after": bundle.retry_after,
            "elapsed_ms": bundle.document.elapsed_ms,
        },
    }
    try:
        metadata_uri = v.require_text(writer(BRONZE_ZONE, ref.metadata_path, canonical_json(metadata)), "metadata_uri")
    except Exception as exc:
        raise RawPersistenceError(
            f"could not persist the raw manifest: {exc}", write_stage="MANIFEST", raw_artifact=artifact
        ) from exc
    return PersistedRawArtifact(artifact=artifact, metadata_uri=metadata_uri)


class RawArtifactUnreadable(RuntimeError):
    """The stored artifact cannot be trusted enough to parse."""


@dataclass(frozen=True)
class LoadedRawArtifact:
    metadata: Mapping[str, Any]
    request: ListingPageRequest
    bundle: FetchBundle
    artifact: RawArtifact
    schema_version: str


def load_raw_artifact(ref: RawArtifactRef, *, reader: Reader) -> LoadedRawArtifact:
    """Read an artifact (v1 or v2) and verify every checksum before anything parses it.

    A corrupted artifact is never handed to an extractor: parsing it would
    launder corruption into records that look genuine.
    """
    raw_metadata = reader(BRONZE_ZONE, ref.metadata_path)
    if raw_metadata is None:
        raise RawArtifactUnreadable(f"no metadata sidecar at {ref.metadata_path}")
    metadata = json.loads(raw_metadata.decode("utf-8"))
    version = metadata.get("schema_version")
    stored = metadata["raw_artifact"]
    if version == RAW_METADATA_SCHEMA_VERSION_V1:
        body = reader(BRONZE_ZONE, ref.body_path)
        if body is None:
            raise RawArtifactUnreadable(f"no stored body at {ref.body_path}")
        actual = hashlib.sha256(body).hexdigest()
        if actual != stored["body_sha256"]:
            raise RawArtifactUnreadable(f"body checksum {actual} does not match the recorded {stored['body_sha256']}")
        stored_parts: tuple[RawPart, ...] = ()
        fetch_parts = (
            FetchPart(stored["request_url"], stored["http_status"], stored["content_type"], body, ROLE_DOCUMENT),
        )
    elif version == RAW_METADATA_SCHEMA_VERSION:
        stored_parts = tuple(RawPart(**part) for part in metadata["parts"])
        actual = manifest_sha256(stored_parts)
        if actual != stored["body_sha256"]:
            raise RawArtifactUnreadable(f"manifest checksum {actual} does not match the recorded {stored['body_sha256']}")
        fetched = []
        for part in stored_parts:
            data = reader(BRONZE_ZONE, ref.part_path(part.index))
            if data is None:
                raise RawArtifactUnreadable(f"no stored part at {ref.part_path(part.index)}")
            try:
                body = gunzip_bytes(data) if part.encoding == "gzip" else data
            except (OSError, EOFError, zlib.error) as exc:
                raise RawArtifactUnreadable(f"stored part {part.index} is not valid gzip, its checksum cannot match: {exc}") from exc
            digest = hashlib.sha256(body).hexdigest()
            if digest != part.sha256:
                raise RawArtifactUnreadable(f"body checksum {digest} does not match the recorded {part.sha256}")
            fetched.append(FetchPart(part.url, part.http_status, part.content_type, body, part.role))
        fetch_parts = tuple(fetched)
    else:
        raise RawArtifactUnreadable(f"unsupported raw metadata schema version: {version!r}")

    fetched_at = datetime.fromisoformat(stored["fetched_at"])
    request = ListingPageRequest(
        marketplace_code=ref.marketplace_code,
        marketplace_id=stored["marketplace_id"],
        target=metadata["request"]["target"],
        page=int(metadata["request"]["page"]),
        resource_type=ResourceType(stored["resource_type"]),
    )
    bundle = FetchBundle(
        request_url=stored["request_url"],
        fetched_at=fetched_at,
        parts=fetch_parts,
        retry_after=metadata.get("response", {}).get("retry_after"),
    )
    artifact = create_raw_artifact(
        marketplace_code=ref.marketplace_code,
        crawl_run_id=stored["crawl_run_id"],
        marketplace_id=stored["marketplace_id"],
        request_url=stored["request_url"],
        resource_type=ResourceType(stored["resource_type"]),
        fetched_at=fetched_at,
        http_status=stored["http_status"],
        content_type=stored["content_type"],
        body_sha256=stored["body_sha256"],
        raw_uri=stored["raw_uri"],
        adapter_version=stored["adapter_version"],
        raw_bytes=int(stored["raw_bytes"]),
        parts=stored_parts,
    )
    return LoadedRawArtifact(metadata, request, bundle, artifact, version)

"""Re-parse a stored raw artifact and prove it yields the same observation.

Phase 7 plan section 12. The parent Brief requires that reparsing the same raw
response produces no duplicate and no divergent canonical observation. This
module is how that claim is checked rather than assumed.

The workflow is read-only by construction. It writes nothing to Silver, Kafka,
Gold or PostgreSQL, which is what makes it safe to point at production data.
A divergence is reported, never repaired: repairing one would be a recovery
decision, and recovery is Phase 8.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote

from common.serialization import serialize_for_wire
from config.marketplace_schema import ResourceType, create_raw_artifact
from crawler.contracts import FetchResult, ListingPageRequest
from crawler.raw_store import RAW_METADATA_SCHEMA_VERSION

IDENTICAL = "IDENTICAL"
DIVERGED = "DIVERGED"
NEW_OBSERVATIONS = "NEW_OBSERVATIONS"
CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
PARSE_FAILED = "PARSE_FAILED"
ADAPTER_VERSION_CHANGED = "ADAPTER_VERSION_CHANGED"

BRONZE_ZONE = "bronze"
SILVER_ZONE = "silver"

Reader = Callable[[str, str], "bytes | None"]


class RawArtifactUnreadable(RuntimeError):
    """The stored artifact cannot be trusted enough to parse."""


def _component(value: str) -> str:
    return quote(value.strip(), safe="-_.~")


@dataclass(frozen=True)
class RawArtifactRef:
    marketplace_code: str
    observed_date: str
    hour: str
    crawl_run_id: str
    raw_artifact_id: str

    @property
    def _base(self) -> str:
        return (
            "marketplace/raw/"
            f"marketplace={_component(self.marketplace_code)}/"
            f"observed_date={self.observed_date}/"
            f"hour={self.hour}/"
            f"crawl_run_id={_component(self.crawl_run_id)}/"
            f"raw_artifact_id={_component(self.raw_artifact_id)}"
        )

    @property
    def body_path(self) -> str:
        return f"{self._base}/body.bin"

    @property
    def metadata_path(self) -> str:
        return f"{self._base}/metadata.json"


@dataclass(frozen=True)
class ReparseOutcome:
    raw_artifact_id: str
    status: str
    observation_ids: tuple[str, ...] = ()
    content_hashes: Mapping[str, str] = field(default_factory=dict)
    diverged_observation_ids: tuple[str, ...] = ()
    error_type: str | None = None
    error_message: str | None = None
    # observation_id -> where the original ingest landed it in Silver.
    silver_paths: Mapping[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == IDENTICAL


def content_hash(observation: Any) -> str:
    """Canonical content hash of one observation, ignoring ``produced_at``.

    ``produced_at`` records when *this* message was built, so a reparse will
    always carry a newer one. Including it would make every reparse look
    divergent and hide the defects this check exists to find. Everything that
    describes the offer, the observation and its raw lineage is hashed.
    """
    document = serialize_for_wire(observation)
    if isinstance(document, dict):
        document = {key: value for key, value in document.items() if key != "produced_at"}
    payload = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_raw_artifact(ref: RawArtifactRef, *, reader: Reader) -> tuple[Mapping[str, Any], bytes]:
    """Read the sidecar and the body, verifying the checksum before anything else.

    A corrupted artifact is never handed to a parser. Parsing it would launder
    corruption into Silver-shaped output that looks canonical.
    """
    raw_metadata = reader(BRONZE_ZONE, ref.metadata_path)
    if raw_metadata is None:
        raise RawArtifactUnreadable(f"no metadata sidecar at {ref.metadata_path}")
    metadata = json.loads(raw_metadata.decode("utf-8"))
    version = metadata.get("schema_version")
    if version != RAW_METADATA_SCHEMA_VERSION:
        raise RawArtifactUnreadable(f"unsupported raw metadata schema version: {version!r}")

    body = reader(BRONZE_ZONE, ref.body_path)
    if body is None:
        raise RawArtifactUnreadable(f"no stored body at {ref.body_path}")

    expected = metadata["raw_artifact"]["body_sha256"]
    actual = hashlib.sha256(body).hexdigest()
    if actual != expected:
        raise RawArtifactUnreadable(f"body checksum {actual} does not match the recorded {expected}")
    return metadata, body


def _rebuild(ref: RawArtifactRef, metadata: Mapping[str, Any], body: bytes):
    """Rebuild the request, fetch result and artifact from the sidecar alone.

    Nothing is taken from a caller argument, so a reparse cannot be aimed at
    the wrong target by a mistyped command line.
    """
    stored = metadata["raw_artifact"]
    fetched_at = datetime.fromisoformat(stored["fetched_at"])
    request = ListingPageRequest(
        marketplace_code=ref.marketplace_code,
        marketplace_id=stored["marketplace_id"],
        target=metadata["request"]["target"],
        page=int(metadata["request"]["page"]),
        resource_type=ResourceType(stored["resource_type"]),
    )
    fetch_result = FetchResult(
        request_url=stored["request_url"],
        fetched_at=fetched_at,
        http_status=stored["http_status"],
        content_type=stored["content_type"],
        body=body,
        retry_after=metadata.get("response", {}).get("retry_after"),
        elapsed_ms=metadata.get("response", {}).get("elapsed_ms"),
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
    )
    return request, fetch_result, artifact


def reparse_raw_artifact(
    ref: RawArtifactRef, *, adapter, reader: Reader, clock: Callable[[], datetime],
    silver_dataset: str | None = None,
) -> ReparseOutcome:
    """Re-run the adapter over a stored body in the mandatory order."""
    try:
        metadata, body = load_raw_artifact(ref, reader=reader)
    except RawArtifactUnreadable as error:
        status = CHECKSUM_MISMATCH if "checksum" in str(error) else PARSE_FAILED
        return ReparseOutcome(ref.raw_artifact_id, status, error_type=type(error).__name__, error_message=str(error))

    stored_version = metadata["raw_artifact"]["adapter_version"]
    if stored_version != getattr(adapter, "adapter_version", None):
        # Reparsing under a different adapter is a Silver version change, not a
        # verification. Superseding stored observations needs a schema bump.
        return ReparseOutcome(
            ref.raw_artifact_id, ADAPTER_VERSION_CHANGED,
            error_type="AdapterVersionChanged",
            error_message=f"artifact was parsed by {stored_version}, adapter is {getattr(adapter, 'adapter_version', None)}",
        )

    request, fetch_result, artifact = _rebuild(ref, metadata, body)
    try:
        parsed = adapter.parse_listing_page(
            request=request,
            fetch_result=fetch_result,
            raw_artifact=artifact,
            crawl_run_id=artifact.crawl_run_id,
            # An envelope field only. No observation value is ever taken from
            # here, and content_hash() drops it before hashing.
            produced_at=clock(),
        )
    except Exception as error:
        return ReparseOutcome(ref.raw_artifact_id, PARSE_FAILED, error_type=type(error).__name__, error_message=str(error))

    from data_ingestion.marketplace_silver_sink import silver_observation_path

    hashes = {observation.event_id: content_hash(observation) for observation in parsed.observations}
    paths = {
        observation.event_id: silver_observation_path(observation, *([silver_dataset] if silver_dataset else []))
        for observation in parsed.observations
    }
    return ReparseOutcome(
        ref.raw_artifact_id, IDENTICAL,
        observation_ids=tuple(sorted(hashes)),
        content_hashes=hashes,
        silver_paths=paths,
    )


def read_silver_hashes(outcome: ReparseOutcome, *, reader: Reader) -> dict[str, str]:
    """Content hashes of the Silver records a reparse should match.

    Silver holds one object per observation at a path derived from the event,
    so the existing side is a handful of direct reads, not a scan. An absent
    object is simply missing from the result, which ``classify`` reports as
    ``NEW_OBSERVATIONS``.
    """
    existing = {}
    for observation_id, path in outcome.silver_paths.items():
        payload = reader(SILVER_ZONE, path)
        if payload is not None:
            existing[observation_id] = content_hash(json.loads(payload.decode("utf-8")))
    return existing


def diff_reparsed_observations(
    reparsed: Mapping[str, str], existing: Mapping[str, str]
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Compare two ``observation_id -> content hash`` maps.

    Returns the diverged IDs and the IDs absent from ``existing``, both sorted.
    A pure dict comparison, so the determinism claim is checkable in
    milliseconds without Spark or storage.
    """
    diverged = sorted(key for key, value in reparsed.items() if key in existing and existing[key] != value)
    unseen = sorted(key for key in reparsed if key not in existing)
    return tuple(diverged), tuple(unseen)


def classify(outcome: ReparseOutcome, existing: Mapping[str, str]) -> ReparseOutcome:
    """Grade a successful reparse against what Silver already holds."""
    if outcome.status != IDENTICAL:
        return outcome
    diverged, unseen = diff_reparsed_observations(outcome.content_hashes, existing)
    if diverged:
        return ReparseOutcome(
            outcome.raw_artifact_id, DIVERGED, outcome.observation_ids, outcome.content_hashes,
            diverged_observation_ids=diverged,
            error_type="ContentDiverged",
            error_message=f"{len(diverged)} observation(s) parsed to different content",
        )
    if unseen:
        # The original ingest lost rows. Writing them back is a Phase 8
        # recovery decision, not something a verification tool should do.
        return ReparseOutcome(
            outcome.raw_artifact_id, NEW_OBSERVATIONS, outcome.observation_ids, outcome.content_hashes,
            diverged_observation_ids=unseen,
            error_type="ObservationsMissingFromSilver",
            error_message=f"{len(unseen)} observation(s) absent from Silver",
        )
    return outcome


def reparse_batch(
    refs: Sequence[RawArtifactRef],
    *,
    adapter_for: Callable[[str], Any],
    reader: Reader,
    clock: Callable[[], datetime],
    existing: Mapping[str, str] | None = None,
    silver_reader: Reader | None = None,
    silver_dataset: str | None = None,
) -> tuple[ReparseOutcome, ...]:
    """Reparse every artifact and grade it against Silver.

    The existing side comes from ``existing`` when given, otherwise from
    reading Silver through ``silver_reader``. With neither, an outcome is
    ungraded, which only a unit test should ever want.
    """
    outcomes = []
    for ref in refs:
        outcome = reparse_raw_artifact(
            ref, adapter=adapter_for(ref.marketplace_code), reader=reader, clock=clock, silver_dataset=silver_dataset,
        )
        if existing is not None:
            outcome = classify(outcome, existing)
        elif silver_reader is not None:
            outcome = classify(outcome, read_silver_hashes(outcome, reader=silver_reader))
        outcomes.append(outcome)
    return tuple(outcomes)


def summarise(outcomes: Sequence[ReparseOutcome], *, reported_at: datetime) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for outcome in outcomes:
        counts[outcome.status] = counts.get(outcome.status, 0) + 1
    problems = [
        {
            "raw_artifact_id": outcome.raw_artifact_id,
            "status": outcome.status,
            "observation_ids": list(outcome.diverged_observation_ids),
            "error_type": outcome.error_type,
        }
        for outcome in outcomes
        if not outcome.ok
    ]
    return {
        "reported_at": reported_at.astimezone(timezone.utc).isoformat(),
        "artifacts": len(outcomes),
        "counts": dict(sorted(counts.items())),
        "problems": problems,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify that a stored raw artifact reparses unchanged")
    parser.add_argument("--marketplace", required=True)
    parser.add_argument("--observed-date", required=True)
    parser.add_argument("--hour", required=True)
    parser.add_argument("--crawl-run-id", required=True)
    parser.add_argument("--raw-artifact-id", required=True, action="append")
    parser.add_argument("--silver-dataset", default=None, help="Silver dataset prefix the ingest landed observations under")
    parser.add_argument("--report-path", default=None, help="also write the report to this local file")
    args = parser.parse_args(argv)

    import common.object_store as object_store
    from crawler.sites.tiki import TikiCrawler

    adapters = {"tiki": TikiCrawler}
    refs = [
        RawArtifactRef(args.marketplace, args.observed_date, args.hour, args.crawl_run_id, artifact_id)
        for artifact_id in args.raw_artifact_id
    ]
    outcomes = reparse_batch(
        refs,
        # Parse-only: no categories to crawl, and no robots.txt fetch, so a
        # verification run never reaches the marketplace.
        adapter_for=lambda code: adapters[code](categories=[], fetch_robots=False),
        reader=object_store.get_bytes,
        clock=lambda: datetime.now(timezone.utc),
        silver_reader=object_store.get_bytes,
        silver_dataset=args.silver_dataset,
    )
    report = json.dumps(summarise(outcomes, reported_at=datetime.now(timezone.utc)), ensure_ascii=False, sort_keys=True)
    print(report)
    if args.report_path:
        from pathlib import Path

        Path(args.report_path).write_text(report, encoding="utf-8")
    return 0 if all(outcome.ok for outcome in outcomes) else 1


if __name__ == "__main__":
    raise SystemExit(main())

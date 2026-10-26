"""Re-extract a stored raw artifact and prove it yields the same records.

Raw responses are read back (v1 or v2), their checksums verified, the source's
extractor run over them, and the resulting records compared with the ones
Bronze/records holds for that artifact. The workflow is read-only by
construction: it writes nothing to Bronze, Silver, Kafka, Gold or PostgreSQL,
which is what makes it safe to point at production data. A divergence is
reported, never repaired: repairing one is a recovery decision.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ecomdata.domain.records import SourceRecord, build_records, record_json
from ecomdata.ingestion.raw_store import RawArtifactRef, RawArtifactUnreadable, load_raw_artifact
from ecomdata.ingestion.record_store import read_records, records_path

IDENTICAL = "IDENTICAL"
DIVERGED = "DIVERGED"
RECORDS_MISSING = "RECORDS_MISSING"
CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
PARSE_FAILED = "PARSE_FAILED"
ADAPTER_VERSION_CHANGED = "ADAPTER_VERSION_CHANGED"

Reader = Callable[[str, str], "bytes | None"]


@dataclass(frozen=True)
class ReparseOutcome:
    raw_artifact_id: str
    status: str
    record_ids: tuple[str, ...] = ()
    content_hashes: Mapping[str, str] = field(default_factory=dict)
    diverged_record_ids: tuple[str, ...] = ()
    error_type: str | None = None
    error_message: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == IDENTICAL


def content_hash(record: SourceRecord) -> str:
    """Hash of one record's canonical JSON: envelope, lineage and payload."""
    return hashlib.sha256(record_json(record).encode("utf-8")).hexdigest()


def diff_records(reparsed: Mapping[str, str], stored: Mapping[str, str]) -> tuple[str, ...]:
    """Record ids whose content differs, or that exist on one side only, sorted."""
    return tuple(sorted({key for key in reparsed.keys() | stored.keys() if reparsed.get(key) != stored.get(key)}))


def reparse_raw_artifact(
    ref: RawArtifactRef, *, spec: Any, extractor: Any, reader: Reader
) -> ReparseOutcome:
    """Re-run the extractor over a stored artifact and grade it against Bronze/records."""
    try:
        loaded = load_raw_artifact(ref, reader=reader)
    except RawArtifactUnreadable as error:
        status = CHECKSUM_MISMATCH if "checksum" in str(error) else PARSE_FAILED
        return ReparseOutcome(ref.raw_artifact_id, status, error_type=type(error).__name__, error_message=str(error))

    stored_version = loaded.artifact.adapter_version
    if stored_version != spec.adapter_version:
        # Extracting under a different version is a Silver version change, not
        # a verification.
        return ReparseOutcome(
            ref.raw_artifact_id,
            ADAPTER_VERSION_CHANGED,
            error_type="AdapterVersionChanged",
            error_message=f"artifact was extracted by {stored_version}, the extractor is {spec.adapter_version}",
        )

    try:
        extracted = extractor.extract(loaded.bundle)
        records = build_records(
            spec,
            request=loaded.request,
            raw_artifact=loaded.artifact,
            rows=extracted.rows,
            page_context=extracted.page_context,
        )
    except Exception as error:
        return ReparseOutcome(
            ref.raw_artifact_id, PARSE_FAILED, error_type=type(error).__name__, error_message=str(error)
        )

    hashes = {record.record_id: content_hash(record) for record in records}
    path = records_path(spec.code, loaded.artifact.fetched_at, loaded.artifact.raw_artifact_id)
    stored = read_records(path, reader=reader)
    if stored is None:
        return ReparseOutcome(
            ref.raw_artifact_id,
            RECORDS_MISSING,
            tuple(sorted(hashes)),
            hashes,
            diverged_record_ids=tuple(sorted(hashes)),
            error_type="RecordsMissingFromBronze",
            error_message=f"no records file at {path}",
        )
    diverged = diff_records(hashes, {record.record_id: content_hash(record) for record in stored})
    if diverged:
        return ReparseOutcome(
            ref.raw_artifact_id,
            DIVERGED,
            tuple(sorted(hashes)),
            hashes,
            diverged_record_ids=diverged,
            error_type="ContentDiverged",
            error_message=f"{len(diverged)} record(s) extracted to different content",
        )
    return ReparseOutcome(ref.raw_artifact_id, IDENTICAL, tuple(sorted(hashes)), hashes)


def reparse_batch(
    refs: Sequence[RawArtifactRef],
    *,
    spec_for: Callable[[str], Any],
    extractor_for: Callable[[str], Any],
    reader: Reader,
) -> tuple[ReparseOutcome, ...]:
    return tuple(
        reparse_raw_artifact(
            ref, spec=spec_for(ref.marketplace_code), extractor=extractor_for(ref.marketplace_code), reader=reader
        )
        for ref in refs
    )


def summarise(outcomes: Sequence[ReparseOutcome], *, reported_at: datetime) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for outcome in outcomes:
        counts[outcome.status] = counts.get(outcome.status, 0) + 1
    problems = [
        {
            "raw_artifact_id": outcome.raw_artifact_id,
            "status": outcome.status,
            "record_ids": list(outcome.diverged_record_ids),
            "error_type": outcome.error_type,
        }
        for outcome in outcomes
        if not outcome.ok
    ]
    return {
        "reported_at": reported_at.astimezone(UTC).isoformat(),
        "artifacts": len(outcomes),
        "counts": dict(sorted(counts.items())),
        "problems": problems,
    }


def main(argv: Sequence[str] | None = None, *, reader: Reader | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify that a stored raw artifact re-extracts unchanged")
    parser.add_argument("--marketplace", "--source", dest="marketplace", required=True)
    parser.add_argument("--observed-date", required=True)
    parser.add_argument("--hour", required=True)
    parser.add_argument("--crawl-run-id", required=True)
    parser.add_argument("--raw-artifact-id", required=True, action="append")
    parser.add_argument("--report-path", default=None, help="also write the report to this local file")
    args = parser.parse_args(argv)

    from ecomdata.domain.sources import SOURCES

    if reader is None:
        from ecomdata.ingestion import bootstrap

        reader = bootstrap.object_store().get

    refs = [
        RawArtifactRef(args.marketplace, args.observed_date, args.hour, args.crawl_run_id, artifact_id)
        for artifact_id in args.raw_artifact_id
    ]

    def extractor_for(code: str) -> Any:
        extractor = SOURCES.extractor(code)
        return extractor() if isinstance(extractor, type) else extractor

    # Parse-only: nothing is fetched, so a verification run never reaches the marketplace.
    outcomes = reparse_batch(refs, spec_for=SOURCES.get, extractor_for=extractor_for, reader=reader)
    report = json.dumps(summarise(outcomes, reported_at=datetime.now(UTC)), ensure_ascii=False, sort_keys=True)
    print(report)
    if args.report_path:
        from pathlib import Path

        Path(args.report_path).write_text(report, encoding="utf-8")
    return 0 if all(outcome.ok for outcome in outcomes) else 1


if __name__ == "__main__":
    raise SystemExit(main())

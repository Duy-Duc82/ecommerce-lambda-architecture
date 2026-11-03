"""The Gold publish manifest and the promoted ``current`` pointer.

Phase 7 plan section 11. A run manifest is written for every run, including one
the quality gate refused — a refused run must leave a readable record of what
it produced and why it was turned away. Only the pointer is gated.

The pointer is the single definition of "the last good published version". It
is replaced by one write of the whole manifest document rather than a reference
to another object, so a reader never observes a torn pointer, and
``current.json`` is byte-identical to the run manifest it promoted.

Nothing here reads a wall clock. ``created_at`` is the run's ``as_of``, so two
runs of the same context serialise to the same bytes and a replay can be
checked by comparing files. Wall-clock timestamps belong in
``audit.marketplace_batch_run``.

Schema v2 adds ``dw_versions``: the Delta version of every star table the run
built and judged. The pointer therefore also names the last certified star, and
a run that fails the gate restores the star to it. A v1 manifest still parses,
with no versions.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from ecomdata.domain.datasets import GOLD_DATASET, MARTS

if TYPE_CHECKING:
    from ecomdata.processing.batch.context import GoldWriteResult, MarketplaceBatchContext
    from ecomdata.processing.batch.quality import QualityDecision

GOLD_ZONE = "gold"
MANIFEST_SCHEMA_VERSION = "marketplace-gold-manifest.v2"
READABLE_MANIFEST_VERSIONS = ("marketplace-gold-manifest.v1", MANIFEST_SCHEMA_VERSION)
CURRENT_POINTER_PATH = f"{GOLD_DATASET}/current.json"

PROMOTED = "PROMOTED"
ALREADY_CURRENT = "ALREADY_CURRENT"
QUALITY_FAILED = "QUALITY_FAILED"
BACKFILL_REFUSED = "BACKFILL_REFUSED"
PROMOTION_CONFLICT = "PROMOTION_CONFLICT"

Writer = Callable[[str, str, bytes], str]
Reader = Callable[[str, str], "bytes | None"]

# The same registry entries write_run_scoped_gold partitions by, so a consumer
# can find a dataset's layout without listing the object store.
_DAILY_PARTITIONS = {d.name: d.partition_columns for d in MARTS if d.partition_columns}


def run_manifest_path(run_id: str) -> str:
    return f"{GOLD_DATASET}/manifests/run_id={quote(run_id, safe='')}/manifest.json"


@dataclass(frozen=True)
class ManifestDataset:
    dataset_name: str
    uri: str
    row_count: int
    partition_columns: tuple[str, ...]


@dataclass(frozen=True)
class GoldManifest:
    manifest_schema_version: str
    run_id: str
    as_of: datetime
    silver_uri: str
    gold_run_uri: str
    rule_versions: Mapping[str, str]
    datasets: tuple[ManifestDataset, ...]
    quality: Mapping[str, Any]
    previous_run_id: str | None
    dw_versions: Mapping[str, int] = field(default_factory=dict)

    @property
    def created_at(self) -> datetime:
        """Equal to ``as_of`` by construction; see the module docstring."""
        return self.as_of


@dataclass(frozen=True)
class PromotionResult:
    promoted: bool
    reason: str
    manifest_uri: str | None
    previous_run_id: str | None


def build_gold_manifest(
    writes: Mapping[str, GoldWriteResult],
    decision: QualityDecision,
    context: MarketplaceBatchContext,
    *,
    previous_run_id: str | None,
    dw_versions: Mapping[str, int | None] | None = None,
) -> GoldManifest:
    if decision.run_id != context.run_id:
        raise ValueError("decision belongs to another run")
    if not writes:
        raise ValueError("a manifest needs at least one written dataset")
    datasets = tuple(
        ManifestDataset(
            dataset_name=name,
            uri=writes[name].uri,
            row_count=int(writes[name].row_count),
            partition_columns=_DAILY_PARTITIONS.get(name, ()),
        )
        for name in sorted(writes)
    )
    gold_run_uri = context.gold_run_uri
    return GoldManifest(
        manifest_schema_version=MANIFEST_SCHEMA_VERSION,
        run_id=context.run_id,
        as_of=context.as_of,
        silver_uri=context.silver_uri,
        gold_run_uri=gold_run_uri,
        rule_versions={
            "anomaly": context.anomaly_rule_version,
            "counter": context.counter_rule_version,
            "freshness": context.freshness_rule_version,
            "quality": context.quality_rule_version,
        },
        datasets=datasets,
        quality={
            "status": "PASS" if decision.passed else "FAIL",
            "rule_version": decision.rule_version,
            "mandatory_total": decision.mandatory_total,
            "mandatory_failures": decision.mandatory_failures,
            "advisory_failures": decision.advisory_failures,
            "skipped": decision.skipped,
        },
        previous_run_id=previous_run_id,
        dw_versions={name: int(v) for name, v in sorted((dw_versions or {}).items()) if v is not None},
    )


def _instant(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def serialize_manifest(manifest: GoldManifest) -> bytes:
    """Canonical JSON: UTF-8, sorted keys, no spacing, no trailing newline.

    The same canonicalisation ``ecomdata.ingestion.raw_store`` uses, so the two halves of
    the pipeline never disagree about what "canonical" means.
    """
    payload = {
        "manifest_schema_version": manifest.manifest_schema_version,
        "run_id": manifest.run_id,
        "as_of": _instant(manifest.as_of),
        "created_at": _instant(manifest.created_at),
        "silver_uri": manifest.silver_uri,
        "gold_run_uri": manifest.gold_run_uri,
        "rule_versions": dict(manifest.rule_versions),
        "datasets": [
            {
                "dataset_name": dataset.dataset_name,
                "uri": dataset.uri,
                "row_count": dataset.row_count,
                "partition_columns": list(dataset.partition_columns),
            }
            for dataset in manifest.datasets
        ],
        "quality": dict(manifest.quality),
        "previous_run_id": manifest.previous_run_id,
        "dw_versions": dict(sorted(manifest.dw_versions.items())),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def parse_manifest(payload: bytes) -> GoldManifest:
    document = json.loads(payload.decode("utf-8"))
    version = document.get("manifest_schema_version")
    if version not in READABLE_MANIFEST_VERSIONS:
        raise ValueError(f"unsupported manifest schema version: {version!r}")
    return GoldManifest(
        manifest_schema_version=version,
        run_id=document["run_id"],
        as_of=datetime.fromisoformat(document["as_of"]),
        silver_uri=document["silver_uri"],
        gold_run_uri=document["gold_run_uri"],
        rule_versions=dict(document["rule_versions"]),
        datasets=tuple(
            ManifestDataset(
                dataset_name=entry["dataset_name"],
                uri=entry["uri"],
                row_count=int(entry["row_count"]),
                partition_columns=tuple(entry["partition_columns"]),
            )
            for entry in document["datasets"]
        ),
        quality=dict(document["quality"]),
        previous_run_id=document["previous_run_id"],
        dw_versions={name: int(v) for name, v in document.get("dw_versions", {}).items()},
    )


def write_run_manifest(manifest: GoldManifest, *, writer: Writer) -> str:
    """Record what this run produced. Always called, gate or no gate."""
    return writer(GOLD_ZONE, run_manifest_path(manifest.run_id), serialize_manifest(manifest))


def read_current_manifest(*, reader: Reader) -> GoldManifest | None:
    payload = reader(GOLD_ZONE, CURRENT_POINTER_PATH)
    if payload is None:
        return None
    return parse_manifest(payload)


def promote_manifest(
    manifest: GoldManifest,
    *,
    writer: Writer,
    reader: Reader,
    allow_backfill: bool = False,
    expected_current_run_id: str | None,
) -> PromotionResult:
    """Advance the ``current`` pointer, but only when the run earned it.

    ``expected_current_run_id`` is the pointer's run ID as the caller read it,
    ``None`` for no pointer. It is required: if the pointer has moved since,
    another run promoted in between, and moving it again would replace that
    run's version with one judged against an older predecessor. Nothing is
    written then, not even the run manifest. The batch lock should make this
    unreachable; this is the guard for a caller outside the lock.

    Otherwise the run manifest is written unconditionally, and what follows
    decides only whether the pointer moves.
    """
    current = read_current_manifest(reader=reader)
    previous_run_id = current.run_id if current else None
    if previous_run_id != expected_current_run_id:
        return PromotionResult(False, PROMOTION_CONFLICT, None, previous_run_id)
    manifest_uri = write_run_manifest(manifest, writer=writer)

    refusal = promotion_refusal(manifest, current, allow_backfill=allow_backfill)
    if refusal is not None:
        return PromotionResult(False, refusal, manifest_uri, previous_run_id)

    writer(GOLD_ZONE, CURRENT_POINTER_PATH, serialize_manifest(manifest))
    return PromotionResult(True, PROMOTED, manifest_uri, previous_run_id)


def promotion_refusal(
    manifest: GoldManifest,
    current: GoldManifest | None,
    *,
    allow_backfill: bool = False,
) -> str | None:
    """Why the pointer would not move to ``manifest``, or None if it would.

    Pure, and shared with the orchestrator, which must know the answer before
    it publishes the cache: the cache has no notion of time, so a refusal
    discovered only after publication would leave it serving an older window
    than the pointer names.
    """
    if manifest.quality.get("status") != "PASS":
        return QUALITY_FAILED
    if current is not None and current.run_id == manifest.run_id:
        # Re-promoting the run that is already current writes nothing. The
        # pointer already holds these exact bytes.
        return ALREADY_CURRENT
    if current is not None and current.as_of > manifest.as_of and not allow_backfill:
        # A reprocessed older window must not silently become the serving
        # version. Say so and let the operator opt in.
        return BACKFILL_REFUSED
    return None


def manifest_chain(manifests: Sequence[GoldManifest]) -> tuple[str, ...]:
    """Run IDs newest first, following ``previous_run_id`` backwards."""
    by_run = {manifest.run_id: manifest for manifest in manifests}
    if not manifests:
        return ()
    chain: list[str] = []
    cursor: str | None = manifests[0].run_id
    while cursor and cursor not in chain:
        chain.append(cursor)
        found = by_run.get(cursor)
        cursor = found.previous_run_id if found else None
    return tuple(chain)

"""The batch pipeline through its real stages: silver, marts, quality, publish, promote.

Silver and Gold are DataFrames in memory (``MemoryLake``), the audit, the quality repository and the object
store are the in-memory ports; Spark transforms, the quality rules and the manifest logic run for real.
"""
from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.datasets import MART_NAMES
from ecomdata.processing.batch.context import MarketplaceBatchContext
from ecomdata.processing.batch.manifest import GOLD_ZONE, read_current_manifest
from ecomdata.processing.batch.memory import MemoryAudit, MemoryQuality, MemoryStore
from ecomdata.processing.batch.quality import QualityGateFailure
from ecomdata.processing.batch.runner import BatchDependencies, default_stages, run_batch, select_stages
from ecomdata.processing.batch.stages import marts as marts_stage
from ecomdata.processing.batch.stages.base import RunOptions
from tests.spark.lake_support import MemoryLake, lines_of
from tests.spark.test_marketplace_marts import _audit
from tests.spark.test_normalize import lazada_records, tiki_records
from tests.spark_support import requires_spark

pytestmark = requires_spark

AS_OF = datetime(2026, 10, 25, 10, tzinfo=UTC)


class RecordingPublisher:
    def __init__(self):
        self.published = []

    def stage(self, marts, *, run_id):
        return [name for name in sorted(marts)]

    def publish(self, staged, *, run_id, published_at, quality, manifest_uri):
        self.published.append((run_id, len(staged), quality.passed, manifest_uri))

    def cleanup(self, staged):
        pass


def pipeline(spark, lines, *, parsed=5, run_id="run-1", extra=None):
    lake, store, audit = MemoryLake(spark, lines), MemoryStore(), MemoryAudit()
    publisher = RecordingPublisher()
    attempts_rows = [("run-1", "tiki", date(2026, 10, 25), "SUCCEEDED", 100, 10, parsed, 0, None)]
    deps = BatchDependencies(
        audit=audit,
        quality=MemoryQuality(),
        store=store,
        io=lake,
        spark_provider=lambda: spark,
        crawl_audit=lambda session: _audit(spark, attempts_rows),
        publisher=publisher,
        clock=lambda: AS_OF,
    )
    context = MarketplaceBatchContext(run_id, AS_OF, "memory://silver", "memory://gold")
    return context, deps, lake, store, audit, publisher


def run_all(spark, lines, **kwargs):
    context, deps, lake, store, audit, publisher = pipeline(spark, lines, **kwargs)
    report = run_batch(context, deps=deps, stages=default_stages(), options=RunOptions(serving_gold_root_uri="memory://gold"))
    return report, context, deps, lake, store, audit, publisher


def test_the_five_stages_take_a_run_from_bronze_records_to_a_promoted_gold(spark):
    report, context, deps, lake, store, audit, publisher = run_all(spark, lines_of(tiki_records()))

    record = audit.get("run-1")
    assert record.status is S.PROMOTED
    assert [(name, status) for name, status in audit.history[1:]] == [
        ("run-1", S.SILVER_BUILT), ("run-1", S.GOLD_WRITTEN), ("run-1", S.QUALITY_PASSED),
        ("run-1", S.PUBLISHED), ("run-1", S.PROMOTED),
    ]  # fmt: skip
    # 4 organic rows reach the marts; the sponsored one stays in Silver only.
    assert record.silver_rows == 4
    assert set(record.dataset_counts) == set(MART_NAMES)
    assert record.dataset_counts["offer_current"] == 4
    assert record.quality_status == "PASS" and record.mandatory_failure_count == 0
    assert record.cache_published and record.manifest_promoted
    assert publisher.published == [("run-1", 10, True, record.manifest_uri)]
    assert read_current_manifest(reader=store.get).run_id == "run-1"
    assert "quality" in [item.stage for item in report.stages]


def test_both_sources_reach_the_marts(spark):
    report, context, deps, lake, store, audit, _ = run_all(spark, lines_of([*tiki_records(), *lazada_records()]), parsed=10)

    assert audit.get("run-1").status is S.PROMOTED
    current = lake.read_gold(context, "offer_current")
    assert {row.marketplace for row in current.select("marketplace").distinct().collect()} == {"tiki", "lazada"}


def test_each_stage_run_alone_leaves_the_same_gold_manifest_and_audit_as_all_of_them(spark):
    lines = lines_of(tiki_records())
    _, all_context, _, all_lake, all_store, all_audit, _ = run_all(spark, lines)

    context, deps, lake, store, audit, _ = pipeline(spark, lines)
    options = RunOptions(serving_gold_root_uri="memory://gold")
    for name in ("silver", "marts", "quality", "publish", "promote"):
        run_batch(context, deps=deps, stages=select_stages([name]), options=options)

    assert audit.get("run-1") == all_audit.get("run-1")
    assert audit.history == all_audit.history
    gold = lambda lk, ctx, name: sorted(map(tuple, lk.read_gold(ctx, name).collect()), key=str)  # noqa: E731
    assert {n: gold(lake, context, n) for n in MART_NAMES} == {n: gold(all_lake, all_context, n) for n in MART_NAMES}
    assert store.objects == all_store.objects


def test_rerunning_a_finished_pipeline_changes_nothing(spark):
    _, context, deps, lake, store, audit, publisher = run_all(spark, lines_of(tiki_records()))
    before = dict(store.objects), list(audit.history)

    report = run_batch(context, deps=deps, stages=default_stages(), options=RunOptions(serving_gold_root_uri="memory://gold"))

    assert all(item.skipped for item in report.stages)
    assert (dict(store.objects), list(audit.history)) == before
    assert len(publisher.published) == 1


def test_a_repeated_observation_id_fails_the_gate_that_used_to_run_after_the_dedup(spark):
    lines = lines_of(tiki_records())
    twins = []
    for line in lines:
        wire = json.loads(line)
        if wire["record_kind"] == "listing_row":
            wire["record_id"] = "twin-" + wire["record_id"]
            wire["raw_artifact_id"] = "twin-" + wire["raw_artifact_id"]
            twins.append(json.dumps(wire))

    context, deps, lake, store, audit, publisher = pipeline(spark, lines + twins, parsed=10)
    with pytest.raises(QualityGateFailure) as refusal:
        run_batch(context, deps=deps, stages=default_stages(), options=RunOptions(serving_gold_root_uri="memory://gold"))

    assert "observation_id_unique" in refusal.value.failing
    record = audit.get("run-1")
    assert record.status is S.QUALITY_FAILED and record.mandatory_failure_count >= 1
    # The evidence is stored, the manifest says FAIL, nothing was published or promoted.
    assert deps.quality.results["run-1"]
    assert publisher.published == [] and read_current_manifest(reader=store.get) is None
    assert json.loads(store.get(GOLD_ZONE, "marketplace/manifests/run_id=run-1/manifest.json"))["quality"]["status"] == "FAIL"


def test_the_marts_summary_records_the_repeats_counted_before_dedup(spark):
    lines = lines_of(tiki_records())
    twins = [
        json.dumps({**wire, "record_id": "twin-" + wire["record_id"], "raw_artifact_id": "twin-" + wire["raw_artifact_id"]})
        for wire in map(json.loads, lines)
        if wire["record_kind"] == "listing_row"
    ]
    context, deps, lake, store, audit, _ = pipeline(spark, lines + twins, parsed=10)

    run_batch(context, deps=deps, stages=select_stages(["silver", "marts"]))

    summary = json.loads(store.get("silver", marts_stage.summary_key("run-1")))
    assert summary["duplicates_before_dedup"] == {"observation_id": 4, "identical_content": 4}
    assert summary["silver_rows"] == 4


def test_a_quality_only_run_writes_gold_and_the_manifest_but_never_serves(spark):
    context, deps, lake, store, audit, publisher = pipeline(spark, lines_of(tiki_records()))

    run_batch(context, deps=deps, stages=default_stages(), options=RunOptions(quality_only=True, serving_gold_root_uri="memory://gold"))

    record = audit.get("run-1")
    assert record.status is S.HELD and record.held_reason == "QUALITY_ONLY"
    assert record.manifest_uri is not None
    assert publisher.published == [] and read_current_manifest(reader=store.get) is None


def test_a_scratch_gold_root_judges_the_run_but_writes_no_manifest(spark):
    context, deps, lake, store, audit, publisher = pipeline(spark, lines_of(tiki_records()))

    run_batch(context, deps=deps, stages=default_stages(), options=RunOptions(serving_gold_root_uri="memory://serving"))

    record = audit.get("run-1")
    assert record.status is S.HELD and record.held_reason == "SCRATCH_GOLD_ROOT"
    assert record.manifest_uri is None
    assert not [key for zone, key in store.objects if zone == GOLD_ZONE]
    assert deps.quality.results["run-1"]

"""Batch context and Gold layout tests, items 34 and 46 of the Phase 6 plan."""
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from batch_layer.marketplace_postgres import DATASETS, MarketplaceCachePublisher
from batch_layer.marketplace_warehouse import (
    MarketplaceBatchContext,
    write_run_scoped_gold,
)

AS_OF = datetime(2026, 9, 5, 10, tzinfo=timezone.utc)


class FakeWriter:
    def __init__(self, log, name):
        self.log = log
        self.name = name
        self.partitions = None
        self.options = {}

    def option(self, key, value):
        self.options[key] = value
        return self

    def mode(self, mode):
        self.mode_value = mode
        return self

    def partitionBy(self, *columns):
        self.partitions = columns
        return self

    def parquet(self, uri):
        self.log.append((self.name, uri, self.mode_value, self.partitions))


class FakeFrame:
    def __init__(self, name, log, columns, rows=1):
        self.name = name
        self.log = log
        self.columns = columns
        self.rows = rows

    def count(self):
        return self.rows

    @property
    def write(self):
        self.writer = FakeWriter(self.log, self.name)
        return self.writer


def _marts(log):
    daily_columns = {
        "offer_price_history_daily": ["marketplace", "observed_date"],
        "offer_change_daily": ["marketplace", "observed_date"],
        "category_price_daily": ["marketplace", "observed_date"],
        "source_coverage_daily": ["marketplace", "observed_date"],
        "counter_delta_daily": ["marketplace", "observed_date"],
        "crawl_reliability_daily": ["marketplace", "request_date"],
    }
    return {
        name: FakeFrame(name, log, daily_columns.get(name, ["marketplace"]))
        for name in DATASETS
    }


def test_context_normalizes_utc_and_validates_run_id():
    context = MarketplaceBatchContext(
        "run_1", AS_OF, "file:///silver", "file:///gold"
    )

    assert context.as_of.tzinfo == timezone.utc


def test_context_converts_an_offset_as_of_to_utc():
    tehran = timezone(timedelta(hours=3, minutes=30))

    context = MarketplaceBatchContext(
        "run_1", AS_OF.astimezone(tehran), "file:///silver", "file:///gold"
    )

    assert context.as_of == AS_OF
    assert context.as_of.tzinfo == timezone.utc


@pytest.mark.parametrize(
    "run_id", ["", " ", "run 1", "run/1", "run;drop", "x" * 65, "réun"]
)
def test_context_rejects_an_unsafe_run_id(run_id):
    with pytest.raises(ValueError, match="run_id"):
        MarketplaceBatchContext(run_id, AS_OF, "file:///silver", "file:///gold")


def test_context_rejects_a_naive_as_of():
    with pytest.raises(ValueError, match="as_of"):
        MarketplaceBatchContext(
            "run-1", datetime(2026, 9, 5, 10), "file:///silver", "file:///gold"
        )


@pytest.mark.parametrize(
    "field", ["silver_uri", "gold_root_uri", "freshness_rule_version", "counter_rule_version"]
)
def test_context_rejects_blank_required_text(field):
    values = {
        "run_id": "run-1",
        "as_of": AS_OF,
        "silver_uri": "file:///silver",
        "gold_root_uri": "file:///gold",
    }
    values[field] = "   "

    with pytest.raises(ValueError, match=field):
        MarketplaceBatchContext(**values)


@pytest.mark.parametrize("field", ["freshness_seconds", "counter_max_gap_seconds"])
def test_context_rejects_non_positive_windows(field):
    with pytest.raises(ValueError, match=field):
        MarketplaceBatchContext(
            "run-1", AS_OF, "file:///silver", "file:///gold", **{field: 0}
        )


# 34
def test_gold_paths_are_scoped_to_the_run_and_never_overwrite_another():
    log = []
    context = MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold")
    other = MarketplaceBatchContext("run-2", AS_OF, "file:///silver", "file:///gold")

    results = write_run_scoped_gold(_marts(log), context)
    first_uris = {name: result.uri for name, result in results.items()}
    other_log = []
    other_uris = {
        name: result.uri
        for name, result in write_run_scoped_gold(_marts(other_log), other).items()
    }

    assert set(results) == set(DATASETS)
    for name, uri in first_uris.items():
        assert uri.startswith("file:///gold/runs/run_id=run-1/")
        assert uri.endswith(f"/{name}")
        assert uri != other_uris[name]


def test_gold_root_trailing_slash_does_not_double_the_separator():
    log = []
    context = MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold/")

    results = write_run_scoped_gold(_marts(log), context)

    for result in results.values():
        assert "//runs" not in result.uri.replace("file://", "")


def test_daily_marts_are_partitioned_by_marketplace_and_their_date_column():
    log = []
    context = MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold")

    write_run_scoped_gold(_marts(log), context)

    partitions = {name: parts for name, _, _, parts in log}
    assert partitions["crawl_reliability_daily"] == ("marketplace", "request_date")
    assert partitions["offer_price_history_daily"] == ("marketplace", "observed_date")
    assert partitions["offer_current"] is None
    assert {mode for _, _, mode, _ in log} == {"overwrite"}


def test_a_rewritten_run_replaces_every_partition_it_held():
    # The session runs with partitionOverwriteMode=dynamic, under which a rerun
    # into the same run directory replaces only the partitions it still has
    # rows for. A resume after fixing future-dated Silver rows then left the
    # rejected partitions on disk, under a manifest whose counts excluded them.
    log = []
    marts = _marts(log)

    write_run_scoped_gold(marts, MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold"))

    assert {name: frame.writer.options.get("partitionOverwriteMode") for name, frame in marts.items()} == {
        name: "static" for name in DATASETS
    }


def test_gold_write_requires_exactly_the_ten_datasets():
    log = []
    context = MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold")
    incomplete = _marts(log)
    del incomplete["offer_freshness"]

    with pytest.raises(ValueError, match="exactly the ten"):
        write_run_scoped_gold(incomplete, context)


def test_staging_table_uses_safe_hash_and_allowlist():
    first = MarketplaceCachePublisher.staging_table("offer_current", "same-run")

    assert first == MarketplaceCachePublisher.staging_table("offer_current", "same-run")
    assert "same-run" not in first


# 46
def test_importing_the_batch_modules_opens_no_client_or_session():
    probe = (
        "import batch_layer.marketplace_warehouse as warehouse;"
        "import batch_layer.marketplace_postgres as publisher;"
        "from pyspark.sql import SparkSession;"
        "assert SparkSession._instantiatedSession is None;"
        "import sys;"
        "assert 'minio' not in sys.modules;"
        "assert 'redis' not in sys.modules;"
        "assert 'elasticsearch' not in sys.modules;"
        "assert len(publisher.DATASETS) == 10;"
        "print('CLEAN')"
    )

    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=300
    )

    assert "CLEAN" in result.stdout, result.stderr


# ----------------------------------------------------------------------------
# Orchestration, Phase 7 plan section 14. Every collaborator is a fake; the
# assertions are about order, because order is the contract here.
# ----------------------------------------------------------------------------
from batch_layer import marketplace_manifest, marketplace_marts, marketplace_postgres, marketplace_quality
from batch_layer import marketplace_warehouse as warehouse
from batch_layer.marketplace_manifest import PROMOTED, PromotionResult
from batch_layer.marketplace_quality import QualityDecision, QualityGateFailure, QualityResult
from batch_layer.marketplace_warehouse import GoldWriteResult, run_marketplace_warehouse

MANIFEST_URI = "s3a://gold/marketplace/manifests/run_id=run-1/manifest.json"


class FakeObservations:
    def cache(self):
        return self

    def count(self):
        return 7

    def unpersist(self):
        return None


class FakeSpark:
    def stop(self):
        return None


class FakeAudit:
    def __init__(self, log):
        self.log = log
        self.quality_failed = None

    def start_run(self, context, started_at, *, resume=False):
        self.log.append("audit.start_run")

    def mark_gold_written(self, **kwargs):
        self.log.append("audit.mark_gold_written")

    def mark_quality_failed(self, **kwargs):
        self.log.append("audit.mark_quality_failed")
        self.quality_failed = kwargs

    def mark_failed(self, **kwargs):
        self.log.append("audit.mark_failed")

    def mark_promotion(self, **kwargs):
        self.log.append("audit.mark_promotion")
        self.promotion = kwargs

    def mark_held(self, **kwargs):
        self.log.append("audit.mark_held")
        self.held = kwargs


class FakeRepository:
    def __init__(self, log):
        self.log = log
        self.recorded = ()

    def record(self, results, *, run_id):
        self.log.append("quality.record")
        self.recorded = tuple(results)


class FakePublisher:
    def __init__(self, log, *, fails=False):
        self.log = log
        self.published = None
        self.fails = fails

    def stage(self, marts, *, run_id):
        self.log.append("publisher.stage")
        return ("staged",)

    def publish(self, staged, *, run_id, published_at, quality, manifest_uri):
        self.log.append("publisher.publish")
        if self.fails:
            raise RuntimeError("duplicate key value violates unique constraint")
        self.published = {"quality": quality, "manifest_uri": manifest_uri}

    def cleanup(self, staged):
        self.log.append("publisher.cleanup")


def decision(*, passed=True, failures=0):
    return QualityDecision(
        run_id="run-1", passed=passed, rule_version="quality-rules.v1", evaluated_at=AS_OF,
        mandatory_total=13, mandatory_failures=failures, advisory_failures=0, skipped=0,
    )


def wire_orchestration(monkeypatch, *, passed=True, publish_fails=False, lock_refused=False):
    log = []
    parts = {"log": log, "lock": []}

    @contextmanager
    def fake_lock():
        # Kept out of ``log``: those assertions describe the run itself.
        if lock_refused:
            from batch_layer.marketplace_lock import BatchAlreadyRunning
            raise BatchAlreadyRunning(820801)
        parts["lock"].append("acquire")
        try:
            yield
        finally:
            parts["lock"].append("release")

    monkeypatch.setattr(warehouse, "default_batch_lock", fake_lock)

    monkeypatch.setattr(warehouse, "build_spark", lambda: FakeSpark())
    monkeypatch.setattr(warehouse, "read_marketplace_silver", lambda spark, uri: "wire")
    monkeypatch.setattr(warehouse, "flatten_marketplace_observations", lambda wire: FakeObservations())
    monkeypatch.setattr(warehouse, "observations_as_of", lambda flat, as_of: flat)
    monkeypatch.setattr(warehouse, "deduplicate_marketplace_observations", lambda flat: flat)
    monkeypatch.setattr(warehouse, "read_crawl_audit", lambda spark: ("attempts", "runs"))
    monkeypatch.setattr(warehouse, "crawl_audit_as_of", lambda attempts, runs, as_of: (attempts, runs))
    monkeypatch.setattr(
        marketplace_marts, "build_marketplace_marts", lambda *a, **k: {name: name for name in DATASETS}
    )
    monkeypatch.setattr(
        warehouse, "write_run_scoped_gold",
        lambda marts, context: {name: GoldWriteResult(name, "file:///gold/" + name, 1) for name in DATASETS},
    )

    verdict = decision(passed=passed, failures=0 if passed else 2)
    evidence = (
        QualityResult(
            run_id="run-1", check_name="observation_id_unique", severity="MANDATORY",
            dataset_name="silver", status="PASS" if passed else "FAIL",
            observed_value=0.0 if passed else 2.0, expectation="an expectation",
            rule_version="quality-rules.v1", failure_sample_json=None, checked_at=AS_OF,
        ),
    )
    parts["results"] = evidence
    monkeypatch.setattr(marketplace_quality, "evaluate_quality_gates", lambda *a, **k: evidence)
    monkeypatch.setattr(marketplace_quality, "decide", lambda *a, **k: verdict)
    parts["decision"] = verdict

    monkeypatch.setattr(marketplace_manifest, "read_current_manifest", lambda **k: None)
    monkeypatch.setattr(marketplace_manifest, "build_gold_manifest", lambda *a, **k: "manifest")

    def fake_write_run_manifest(manifest, *, writer):
        log.append("manifest.write_run")
        return MANIFEST_URI

    def fake_promote(manifest, *, writer, reader, allow_backfill=False):
        log.append("manifest.promote")
        parts["allow_backfill"] = allow_backfill
        return PromotionResult(True, PROMOTED, MANIFEST_URI, None)

    monkeypatch.setattr(marketplace_manifest, "write_run_manifest", fake_write_run_manifest)
    monkeypatch.setattr(marketplace_manifest, "promote_manifest", fake_promote)
    monkeypatch.setattr(marketplace_manifest, "promotion_refusal", lambda manifest, current, **k: None)

    audit, repository, publisher = FakeAudit(log), FakeRepository(log), FakePublisher(log, fails=publish_fails)
    monkeypatch.setattr(marketplace_postgres.MarketplaceBatchAudit, "from_settings", classmethod(lambda cls: audit))
    monkeypatch.setattr(
        marketplace_postgres.MarketplaceQualityRepository, "from_settings", classmethod(lambda cls: repository)
    )
    monkeypatch.setattr(
        marketplace_postgres.MarketplaceCachePublisher, "from_settings", classmethod(lambda cls: publisher)
    )
    parts.update(audit=audit, repository=repository, publisher=publisher)
    return parts


def batch_context():
    return MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold")


def noop_writer(*args):
    return "uri"


def empty_reader(*args):
    return None


def test_a_passing_run_publishes_before_it_promotes(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    result = run_marketplace_warehouse(batch_context(), writer=noop_writer, reader=empty_reader)

    assert parts["log"] == [
        "audit.start_run", "audit.mark_gold_written", "quality.record", "manifest.write_run",
        "publisher.stage", "publisher.publish", "publisher.cleanup", "manifest.promote", "audit.mark_promotion",
    ]
    assert parts["audit"].promotion == {"run_id": "run-1", "promoted": True}
    assert result.status == "SUCCEEDED"
    assert result.manifest_promoted is True
    assert result.manifest_uri == MANIFEST_URI
    assert parts["publisher"].published["quality"] is parts["decision"]
    assert parts["publisher"].published["manifest_uri"] == MANIFEST_URI


# 53, 54, 55
def test_a_refused_run_records_evidence_and_never_reaches_the_cache(monkeypatch):
    parts = wire_orchestration(monkeypatch, passed=False)

    with pytest.raises(QualityGateFailure):
        run_marketplace_warehouse(batch_context(), writer=noop_writer, reader=empty_reader)

    # Results persisted and the manifest written before the refusal propagates.
    assert parts["log"] == [
        "audit.start_run", "audit.mark_gold_written", "quality.record",
        "manifest.write_run", "audit.mark_quality_failed",
    ]
    assert parts["audit"].quality_failed["manifest_uri"] == MANIFEST_URI
    assert parts["audit"].quality_failed["decision"].passed is False


def test_a_refused_run_is_not_also_recorded_as_a_crash(monkeypatch):
    parts = wire_orchestration(monkeypatch, passed=False)

    with pytest.raises(QualityGateFailure):
        run_marketplace_warehouse(batch_context(), writer=noop_writer, reader=empty_reader)

    assert "audit.mark_failed" not in parts["log"]


# 59
def test_quality_only_writes_the_manifest_and_stops(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    result = run_marketplace_warehouse(
        batch_context(), quality_only=True, writer=noop_writer, reader=empty_reader
    )

    # The audit row is closed too: an inspection run must not look hung.
    assert parts["log"] == [
        "audit.start_run", "audit.mark_gold_written", "quality.record", "manifest.write_run", "audit.mark_held",
    ]
    assert parts["audit"].held["reason"] == warehouse.QUALITY_ONLY
    assert parts["audit"].held["manifest_uri"] == MANIFEST_URI
    assert parts["audit"].held["completed_at"] is not None
    assert result.status == "GOLD_WRITTEN"
    assert result.manifest_promoted is False
    assert result.promotion_reason == warehouse.QUALITY_ONLY


def test_allow_backfill_reaches_the_promotion(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    run_marketplace_warehouse(batch_context(), allow_backfill=True, writer=noop_writer, reader=empty_reader)

    assert parts["allow_backfill"] is True


def test_skipping_postgres_still_evaluates_and_promotes(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    result = run_marketplace_warehouse(
        batch_context(), publish_cache=False, writer=noop_writer, reader=empty_reader
    )

    assert parts["log"] == ["manifest.write_run", "manifest.promote"]
    assert result.status == "GOLD_WRITTEN"
    assert result.manifest_promoted is True


# 56
def test_a_publication_failure_leaves_the_manifest_pointer_alone(monkeypatch):
    # Found by a live run against PostgreSQL: the cache transaction rolled back
    # correctly, but the pointer had already advanced, so the serving version
    # named a Gold run whose cache was never published. That is worse than
    # either failure alone, which is why the pointer must move last.
    parts = wire_orchestration(monkeypatch, publish_fails=True)

    with pytest.raises(RuntimeError, match="duplicate key"):
        run_marketplace_warehouse(batch_context(), writer=noop_writer, reader=empty_reader)

    assert "publisher.publish" in parts["log"]
    assert "manifest.promote" not in parts["log"]
    # The run manifest is still written: a failed run stays inspectable.
    assert "manifest.write_run" in parts["log"]


# 56
def test_the_pointer_moves_only_after_the_cache_is_published(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    run_marketplace_warehouse(batch_context(), writer=noop_writer, reader=empty_reader)

    log = parts["log"]
    assert log.index("publisher.publish") < log.index("manifest.promote")


# ----------------------------------------------------------------------------
# A reprocessed older window. The manifest functions are the real ones over a
# dict store, because the defect lives in how their verdict reaches publication.
# ----------------------------------------------------------------------------
REAL_MANIFEST = {
    name: getattr(marketplace_manifest, name)
    for name in (
        "build_gold_manifest", "read_current_manifest", "write_run_manifest", "promote_manifest", "promotion_refusal",
    )
}


class DictStore:
    def __init__(self):
        self.objects = {}

    def write(self, zone, path, data):
        self.objects[(zone, path)] = data
        return f"s3a://{zone}/{path}"

    def read(self, zone, path):
        return self.objects.get((zone, path))


def serving_a_newer_run(monkeypatch):
    for name, function in REAL_MANIFEST.items():
        monkeypatch.setattr(marketplace_manifest, name, function)
    store = DictStore()
    newer = MarketplaceBatchContext("run-2", AS_OF + timedelta(days=2), "file:///silver", "file:///gold")
    verdict = QualityDecision(
        run_id="run-2", passed=True, rule_version="quality-rules.v1", evaluated_at=newer.as_of,
        mandatory_total=13, mandatory_failures=0, advisory_failures=0, skipped=0,
    )
    serving = marketplace_manifest.build_gold_manifest(
        {name: GoldWriteResult(name, "file:///gold/" + name, 1) for name in DATASETS},
        verdict, newer, previous_run_id=None,
    )
    marketplace_manifest.promote_manifest(serving, writer=store.write, reader=store.read)
    return store


def serving_run_id(store):
    return marketplace_manifest.read_current_manifest(reader=store.read).run_id


def test_a_refused_backfill_never_reaches_the_cache(monkeypatch):
    # The pointer refuses to move backwards in time, but the cache publisher
    # has no notion of time at all. Publishing first and asking the pointer
    # afterwards replaced the serving cache with an older window while the
    # pointer still named the newer run, and the run reported SUCCEEDED.
    parts = wire_orchestration(monkeypatch)
    store = serving_a_newer_run(monkeypatch)

    result = run_marketplace_warehouse(batch_context(), writer=store.write, reader=store.read)

    assert "publisher.stage" not in parts["log"]
    assert "publisher.publish" not in parts["log"]
    assert parts["publisher"].published is None
    assert serving_run_id(store) == "run-2"
    assert result.status == "GOLD_WRITTEN"
    assert result.manifest_promoted is False
    assert result.promotion_reason == marketplace_manifest.BACKFILL_REFUSED
    # The run manifest still exists, so the held window stays inspectable.
    assert store.read("gold", marketplace_manifest.run_manifest_path("run-1")) is not None


def test_a_refused_backfill_is_closed_in_the_audit_row(monkeypatch):
    parts = wire_orchestration(monkeypatch)
    store = serving_a_newer_run(monkeypatch)

    run_marketplace_warehouse(batch_context(), writer=store.write, reader=store.read)

    assert parts["log"][-1] == "audit.mark_held"
    assert "audit.mark_failed" not in parts["log"]
    held = parts["audit"].held
    assert held["run_id"] == "run-1"
    assert held["reason"] == marketplace_manifest.BACKFILL_REFUSED
    assert held["manifest_uri"].endswith(marketplace_manifest.run_manifest_path("run-1"))


def test_an_allowed_backfill_publishes_then_moves_the_pointer(monkeypatch):
    parts = wire_orchestration(monkeypatch)
    store = serving_a_newer_run(monkeypatch)

    result = run_marketplace_warehouse(batch_context(), allow_backfill=True, writer=store.write, reader=store.read)

    assert "publisher.publish" in parts["log"]
    assert serving_run_id(store) == "run-1"
    assert result.status == "SUCCEEDED"
    assert result.manifest_promoted is True
    assert result.promotion_reason == marketplace_manifest.PROMOTED


def test_a_refused_backfill_without_postgres_leaves_the_pointer_alone(monkeypatch):
    wire_orchestration(monkeypatch)
    store = serving_a_newer_run(monkeypatch)

    result = run_marketplace_warehouse(batch_context(), publish_cache=False, writer=store.write, reader=store.read)

    assert serving_run_id(store) == "run-2"
    assert result.status == "GOLD_WRITTEN"
    assert result.manifest_promoted is False
    assert result.promotion_reason == marketplace_manifest.BACKFILL_REFUSED


def serving_this_run(monkeypatch, *, previous_run_id):
    """The pointer already names run-1, as after a promotion whose reply was lost."""
    for name, function in REAL_MANIFEST.items():
        monkeypatch.setattr(marketplace_manifest, name, function)
    store = DictStore()
    served = marketplace_manifest.build_gold_manifest(
        {name: GoldWriteResult(name, "file:///gold/" + name, 1) for name in DATASETS},
        decision(), batch_context(), previous_run_id=previous_run_id,
    )
    marketplace_manifest.promote_manifest(served, writer=store.write, reader=store.read)
    return store


def test_rerunning_the_current_run_keeps_its_real_predecessor(monkeypatch):
    # The pointer naming this very run used to become its own predecessor, so
    # the run manifest stopped matching current.json byte for byte and the
    # manifest chain lost every run before it.
    wire_orchestration(monkeypatch)
    store = serving_this_run(monkeypatch, previous_run_id="run-0")
    pointer_before = store.read("gold", marketplace_manifest.CURRENT_POINTER_PATH)

    result = run_marketplace_warehouse(batch_context(), publish_cache=False, writer=store.write, reader=store.read)

    run_manifest = store.read("gold", marketplace_manifest.run_manifest_path("run-1"))
    assert marketplace_manifest.parse_manifest(run_manifest).previous_run_id == "run-0"
    assert run_manifest == pointer_before
    assert store.read("gold", marketplace_manifest.CURRENT_POINTER_PATH) == pointer_before
    assert result.promotion_reason == marketplace_manifest.ALREADY_CURRENT


def test_a_pointer_write_failure_never_records_a_promotion(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    def broken_promote(manifest, *, writer, reader, allow_backfill=False):
        parts["log"].append("manifest.promote")
        raise ConnectionError("object store unreachable")

    monkeypatch.setattr(marketplace_manifest, "promote_manifest", broken_promote)

    with pytest.raises(ConnectionError):
        run_marketplace_warehouse(batch_context(), writer=noop_writer, reader=empty_reader)

    assert "audit.mark_promotion" not in parts["log"]
    assert parts["log"][-1] == "audit.mark_failed"


def test_a_pointer_that_already_names_the_run_counts_as_promoted(monkeypatch):
    parts = wire_orchestration(monkeypatch)
    store = serving_this_run(monkeypatch, previous_run_id="run-0")

    run_marketplace_warehouse(batch_context(), resume=True, writer=store.write, reader=store.read)

    assert parts["audit"].promotion == {"run_id": "run-1", "promoted": True}


# ----------------------------------------------------------------------------
# A scratch Gold root. Manifests and the pointer live at one fixed place in the
# active storage profile, whatever --gold-root-uri says, so a replay into a
# scratch root used to promote the production pointer onto scratch datasets.
# ----------------------------------------------------------------------------
SERVING_ROOT = "file:///gold"


def scratch_context():
    return MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///scratch/gold")


def test_a_scratch_gold_root_never_touches_the_serving_manifests(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    result = run_marketplace_warehouse(
        scratch_context(), serving_gold_root_uri=SERVING_ROOT, writer=noop_writer, reader=empty_reader,
    )

    log = parts["log"]
    assert "manifest.write_run" not in log
    assert "publisher.publish" not in log
    assert "manifest.promote" not in log
    assert log[-1] == "audit.mark_held"
    assert parts["audit"].held["reason"] == warehouse.SCRATCH_GOLD_ROOT
    assert parts["audit"].held["manifest_uri"] is None
    assert result.status == "GOLD_WRITTEN"
    assert result.manifest_uri is None
    assert result.promotion_reason == warehouse.SCRATCH_GOLD_ROOT


def test_a_refused_scratch_run_still_records_evidence_but_no_manifest(monkeypatch):
    parts = wire_orchestration(monkeypatch, passed=False)

    with pytest.raises(QualityGateFailure):
        run_marketplace_warehouse(
            scratch_context(), serving_gold_root_uri=SERVING_ROOT, writer=noop_writer, reader=empty_reader,
        )

    assert "quality.record" in parts["log"]
    assert "manifest.write_run" not in parts["log"]
    assert parts["audit"].quality_failed["manifest_uri"] is None


def test_the_serving_root_matches_regardless_of_a_trailing_slash(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    result = run_marketplace_warehouse(
        batch_context(), serving_gold_root_uri=SERVING_ROOT + "/", writer=noop_writer, reader=empty_reader,
    )

    assert "manifest.promote" in parts["log"]
    assert result.status == "SUCCEEDED"


def test_the_cli_names_the_configured_gold_root_as_the_serving_one(monkeypatch, capsys):
    seen = {}

    def fake_run(context, **kwargs):
        seen.update(kwargs, gold_root_uri=context.gold_root_uri)
        return warehouse.MarketplaceBatchResult(context.run_id, "GOLD_WRITTEN", 0, {})

    monkeypatch.setattr(warehouse, "run_marketplace_warehouse", fake_run)
    monkeypatch.setattr(sys, "argv", [
        "marketplace_warehouse", "--run-id", "run-1", "--as-of", "2026-09-05T10:00:00Z",
        "--gold-root-uri", "file:///scratch/gold",
    ])

    warehouse.main()

    assert seen["gold_root_uri"] == "file:///scratch/gold"
    assert seen["serving_gold_root_uri"] == warehouse.data_lake_uri("gold", warehouse.MARKETPLACE_GOLD_DATASET)


# ----------------------------------------------------------------------------
# The as_of cut. A run is a view of the warehouse as it stood at its as_of, so
# nothing observed or recorded after that instant may reach its marts. Without
# the cut, a backfill over a Silver that kept growing sees the newer rows and
# its own future-tolerance gate refuses it, every time.
# ----------------------------------------------------------------------------
from tests.spark_support import requires_spark

CUT_AS_OF = datetime(2026, 9, 5, 10, tzinfo=timezone.utc)


@requires_spark
def test_observations_after_as_of_never_reach_the_run(spark):
    rows = [
        ("before", CUT_AS_OF - timedelta(days=1)),
        ("at", CUT_AS_OF),
        ("after", CUT_AS_OF + timedelta(seconds=1)),
    ]
    flat = spark.createDataFrame(rows, schema="observation_id string, observed_at timestamp")

    kept = warehouse.observations_as_of(flat, CUT_AS_OF)

    # Inclusive: a row observed exactly at as_of belongs to the run.
    assert sorted(row.observation_id for row in kept.collect()) == ["at", "before"]


@requires_spark
def test_crawl_audit_recorded_after_as_of_never_reaches_the_run(spark):
    attempts = spark.createDataFrame(
        [
            ("run-a", CUT_AS_OF - timedelta(hours=2), CUT_AS_OF - timedelta(hours=1)),
            ("run-b", CUT_AS_OF - timedelta(minutes=5), CUT_AS_OF + timedelta(minutes=5)),
            ("run-c", CUT_AS_OF + timedelta(hours=1), CUT_AS_OF + timedelta(hours=2)),
        ],
        schema="crawl_run_id string, started_at timestamp, completed_at timestamp",
    )
    runs = spark.createDataFrame(
        [
            ("run-a", CUT_AS_OF - timedelta(hours=2)),
            ("run-b", CUT_AS_OF - timedelta(minutes=5)),
            ("run-c", CUT_AS_OF + timedelta(hours=1)),
        ],
        schema="crawl_run_id string, started_at timestamp",
    )

    cut_attempts, cut_runs = warehouse.crawl_audit_as_of(attempts, runs, CUT_AS_OF)

    # An attempt is known once it has completed; one still in flight at as_of
    # had not yet reported anything.
    assert [row.crawl_run_id for row in cut_attempts.collect()] == ["run-a"]
    # A crawl run exists once it has started, finished or not.
    assert sorted(row.crawl_run_id for row in cut_runs.collect()) == ["run-a", "run-b"]


@pytest.mark.parametrize("name", ["observations_as_of", "crawl_audit_as_of"])
def test_the_cut_refuses_a_naive_as_of(name):
    cut = getattr(warehouse, name)
    args = (None, None) if name == "crawl_audit_as_of" else (None,)

    with pytest.raises(ValueError, match="as_of"):
        cut(*args, datetime(2026, 9, 5, 10))


def test_the_run_reads_silver_and_audit_as_of_its_context(monkeypatch):
    parts = wire_orchestration(monkeypatch)
    seen = {}
    flat = FakeObservations()

    monkeypatch.setattr(warehouse, "flatten_marketplace_observations", lambda wire: flat)

    def cut_observations(frame, as_of):
        seen["observations"] = (frame, as_of)
        return "cut-observations"

    def deduplicate(frame):
        seen["deduplicated"] = frame
        return FakeObservations()

    def cut_audit(attempts, runs, as_of):
        seen["audit"] = (attempts, runs, as_of)
        return "cut-attempts", "cut-runs"

    def marts(observations, attempts, runs, context):
        seen["marts"] = (attempts, runs)
        return {name: name for name in DATASETS}

    def gates(observations, marts, attempts, runs, context):
        seen["gates"] = (attempts, runs)
        return parts["results"]

    monkeypatch.setattr(warehouse, "observations_as_of", cut_observations)
    monkeypatch.setattr(warehouse, "deduplicate_marketplace_observations", deduplicate)
    monkeypatch.setattr(warehouse, "crawl_audit_as_of", cut_audit)
    monkeypatch.setattr(marketplace_marts, "build_marketplace_marts", marts)
    monkeypatch.setattr(marketplace_quality, "evaluate_quality_gates", gates)

    run_marketplace_warehouse(batch_context(), writer=noop_writer, reader=empty_reader)

    assert seen["observations"] == (flat, AS_OF)
    # Cut before deduplication, so a row past as_of cannot collide with one
    # inside it and fail a run that never sees it.
    assert seen["deduplicated"] == "cut-observations"
    assert seen["audit"] == ("attempts", "runs", AS_OF)
    assert seen["marts"] == ("cut-attempts", "cut-runs")
    assert seen["gates"] == ("cut-attempts", "cut-runs")


def test_context_rejects_a_reconciliation_lookback_inside_the_settle_delay():
    with pytest.raises(ValueError, match="reconciliation_lookback_seconds"):
        MarketplaceBatchContext(
            "run-1", AS_OF, "file:///silver", "file:///gold",
            reconciliation_settle_seconds=3600, reconciliation_lookback_seconds=3600,
        )


@pytest.mark.parametrize("field", ["reconciliation_settle_seconds", "reconciliation_lookback_seconds"])
def test_context_rejects_non_positive_reconciliation_windows(field):
    with pytest.raises(ValueError, match=field):
        MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold", **{field: 0})

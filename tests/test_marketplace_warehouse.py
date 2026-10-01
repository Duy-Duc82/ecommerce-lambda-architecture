"""Batch context and Gold layout tests, items 34 and 46 of the Phase 6 plan."""
import subprocess
import sys
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
        return FakeWriter(self.log, self.name)


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


def wire_orchestration(monkeypatch, *, passed=True, publish_fails=False):
    log = []
    parts = {"log": log}

    monkeypatch.setattr(warehouse, "build_spark", lambda: FakeSpark())
    monkeypatch.setattr(warehouse, "read_marketplace_silver", lambda spark, uri: "wire")
    monkeypatch.setattr(warehouse, "flatten_marketplace_observations", lambda wire: FakeObservations())
    monkeypatch.setattr(warehouse, "deduplicate_marketplace_observations", lambda flat: flat)
    monkeypatch.setattr(warehouse, "read_crawl_audit", lambda spark: ("attempts", "runs"))
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


def test_a_passing_run_promotes_before_it_publishes(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    result = run_marketplace_warehouse(batch_context(), writer=noop_writer, reader=empty_reader)

    assert parts["log"] == [
        "audit.start_run", "audit.mark_gold_written", "quality.record",
        "manifest.promote", "publisher.stage", "publisher.publish", "publisher.cleanup",
    ]
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

    assert parts["log"] == ["audit.start_run", "audit.mark_gold_written", "quality.record", "manifest.write_run"]
    assert result.status == "GOLD_WRITTEN"
    assert result.manifest_promoted is False


def test_allow_backfill_reaches_the_promotion(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    run_marketplace_warehouse(batch_context(), allow_backfill=True, writer=noop_writer, reader=empty_reader)

    assert parts["allow_backfill"] is True


def test_skipping_postgres_still_evaluates_and_promotes(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    result = run_marketplace_warehouse(
        batch_context(), publish_cache=False, writer=noop_writer, reader=empty_reader
    )

    assert parts["log"] == ["manifest.promote"]
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

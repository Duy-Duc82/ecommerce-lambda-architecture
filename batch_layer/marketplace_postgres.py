"""Run-scoped staging and atomic PostgreSQL publication for marketplace marts."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

import psycopg2

from config.settings import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER
from batch_layer.marketplace_warehouse import MarketplaceBatchContext

if TYPE_CHECKING:
    from batch_layer.marketplace_quality import QualityDecision, QualityResult

DATASET_COLUMNS = {
    "offer_current": ["offer_id", "marketplace", "marketplace_id", "platform_listing_id", "seller_id", "product_title", "brand", "category_path", "source_url", "currency", "active_status", "first_seen_at", "last_seen_at", "current_observation_id", "observed_at", "fetched_at", "current_price", "list_price", "shipping_price", "rating_value", "rating_scale", "rating_count", "review_count", "sold_count", "availability", "ranking_position", "raw_uri", "raw_sha256", "adapter_version", "crawl_run_id"],
    "seller_current": ["seller_id", "marketplace", "marketplace_id", "first_seen_at", "last_seen_at", "observed_offer_count"],
    "offer_price_history_daily": ["marketplace", "offer_id", "observed_date", "currency", "first_observed_at", "last_observed_at", "first_price", "last_price", "min_price", "max_price", "avg_price", "first_list_price", "last_list_price", "observation_count", "distinct_price_count", "last_availability", "last_observation_id"],
    "offer_change_daily": ["marketplace", "offer_id", "observed_date", "currency", "transition_count", "price_change_count", "price_drop_count", "price_increase_count", "absolute_price_change_sum", "signed_price_change_sum", "max_price_drop", "max_price_increase", "rating_change_count", "counter_change_count", "availability_change_count"],
    "offer_freshness": ["as_of", "marketplace", "offer_id", "last_observation_id", "last_observed_at", "age_seconds", "freshness_status", "stale_after_seconds", "freshness_rule_version"],
    "category_price_daily": ["marketplace", "category_path", "observed_date", "currency", "observed_offer_count", "observation_count", "min_price", "p25_price", "median_price", "p75_price", "max_price", "avg_price"],
    "source_coverage_daily": ["marketplace", "observed_date", "eligible_offer_count", "observed_offer_count", "missing_offer_count", "fresh_offer_count", "stale_offer_count", "observation_count", "parsed_count", "rejected_count", "coverage_rate", "rejection_rate", "freshness_rule_version"],
    "crawl_reliability_daily": ["marketplace", "request_date", "request_count", "succeeded_count", "failed_count", "success_rate", "avg_latency_ms", "p95_latency_ms", "raw_bytes", "parsed_count", "rejected_count", "rate_limited_count", "transport_error_count", "server_error_count", "parse_error_count", "validation_error_count"],
    "counter_delta_daily": ["marketplace", "offer_id", "observed_date", "counter_name", "first_value", "last_value", "raw_delta_sum", "valid_delta_sum", "valid_transition_count", "invalid_transition_count", "elapsed_seconds_valid", "velocity_proxy_per_hour", "counter_reset_or_invalid", "invalid_reasons_json", "counter_rule_version"],
    "price_anomaly_daily": ["marketplace", "offer_id", "observed_date", "currency", "evaluated_price", "baseline_sample_size", "baseline_median", "baseline_mad", "baseline_p25", "baseline_p75", "baseline_iqr", "deviation_amount", "deviation_percent", "robust_score", "lower_fence", "upper_fence", "anomaly_method", "anomaly_status", "anomaly_reason", "window_days", "min_samples", "mad_threshold", "iqr_multiplier", "anomaly_rule_version"],
}
DATASETS = tuple(DATASET_COLUMNS)
_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


@dataclass(frozen=True)
class StagedDataset:
    dataset_name: str
    staging_table: str
    row_count: int


class MarketplaceBatchAudit:
    def __init__(self, connection_factory: Callable[[], Any]): self.connection_factory = connection_factory

    @classmethod
    def from_settings(cls):
        return cls(lambda: psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB))

    def start_run(self, context: MarketplaceBatchContext, started_at: datetime, *, resume: bool = False) -> None:
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("SELECT status,run_id,as_of,silver_uri FROM audit.marketplace_batch_run WHERE run_id=%s", (context.run_id,))
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
            if row and row[0] == "SUCCEEDED": raise ValueError(f"run_id already SUCCEEDED: {context.run_id}")
            if row and not resume: raise ValueError("existing run requires explicit resume")
            if row and (str(row[2]) != str(context.as_of) or row[3] != context.silver_uri): raise ValueError("resume context does not match existing run")
            cur.execute("""INSERT INTO audit.marketplace_batch_run(run_id,as_of,silver_uri,started_at,status)
                VALUES (%s,%s,%s,%s,'RUNNING') ON CONFLICT(run_id) DO UPDATE SET status='RUNNING',started_at=EXCLUDED.started_at,error_message=NULL""", (context.run_id, context.as_of, context.silver_uri, started_at))

    def mark_gold_written(self, *, run_id: str, gold_run_uri: str, silver_rows: int, dataset_counts: Mapping[str, int], completed_at: datetime | None) -> None:
        status = "GOLD_WRITTEN"
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("UPDATE audit.marketplace_batch_run SET gold_run_uri=%s,silver_rows=%s,gold_rows=%s,dataset_counts=%s,status=%s,completed_at=%s WHERE run_id=%s", (gold_run_uri, silver_rows, sum(dataset_counts.values()), json.dumps(dict(dataset_counts), sort_keys=True), status, completed_at, run_id))

    def mark_failed(self, *, run_id: str, completed_at: datetime, error: Exception) -> None:
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("UPDATE audit.marketplace_batch_run SET status='FAILED',completed_at=%s,error_message=%s WHERE run_id=%s", (completed_at, str(error)[:2000], run_id))

    def mark_quality_failed(self, *, run_id: str, completed_at: datetime, decision: "QualityDecision", manifest_uri: str | None) -> None:
        # QUALITY_FAILED, not FAILED. The run produced complete, inspectable
        # Gold and was refused; losing that distinction would make a refusal
        # look like a crash in every operational view.
        if decision.run_id != run_id: raise ValueError("decision belongs to another run")
        if decision.passed: raise ValueError("a passing decision cannot be recorded as a quality failure")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("""UPDATE audit.marketplace_batch_run SET status='QUALITY_FAILED',completed_at=%s,
                quality_status='FAIL',mandatory_failure_count=%s,manifest_uri=%s,manifest_promoted=FALSE,cache_published=FALSE
                WHERE run_id=%s""", (completed_at, decision.mandatory_failures, manifest_uri, run_id))

    def mark_promotion(self, *, run_id: str, promoted: bool) -> None:
        # Separate from publication, and after the pointer write: the cache
        # commits first, so only the promotion outcome can say whether it moved.
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("UPDATE audit.marketplace_batch_run SET manifest_promoted=%s WHERE run_id=%s", (promoted, run_id))

    def mark_promotion_refused(self, *, run_id: str, completed_at: datetime, reason: str, manifest_uri: str) -> None:
        # GOLD_WRITTEN, not SUCCEEDED: the run passed its gate but was held back
        # from serving, so it stays resumable once an operator opts in.
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("""UPDATE audit.marketplace_batch_run SET status='GOLD_WRITTEN',completed_at=%s,error_message=%s,
                quality_status='PASS',mandatory_failure_count=0,manifest_uri=%s,manifest_promoted=FALSE,cache_published=FALSE
                WHERE run_id=%s""", (completed_at, f"promotion refused: {reason}", manifest_uri, run_id))


class MarketplaceQualityRepository:
    """Persists one row per rule per run, for refused runs as much as passing ones.

    This runs in its own transaction before any publication decision is acted
    on. A blocked publication that left no stored evidence cannot be told apart
    from a crash.
    """

    _UPSERT = """INSERT INTO audit.marketplace_quality_result
        (run_id,check_name,severity,dataset_name,status,observed_value,expectation,rule_version,failure_sample_json,checked_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (run_id,check_name) DO UPDATE SET
            severity=EXCLUDED.severity,dataset_name=EXCLUDED.dataset_name,status=EXCLUDED.status,
            observed_value=EXCLUDED.observed_value,expectation=EXCLUDED.expectation,
            rule_version=EXCLUDED.rule_version,failure_sample_json=EXCLUDED.failure_sample_json,
            checked_at=EXCLUDED.checked_at"""

    _SUMMARY = """SELECT
            COUNT(*) FILTER (WHERE severity='MANDATORY'),
            COUNT(*) FILTER (WHERE severity='MANDATORY' AND status <> 'PASS'),
            COUNT(*) FILTER (WHERE severity='ADVISORY' AND status='FAIL'),
            COUNT(*) FILTER (WHERE status='SKIPPED'),
            MIN(rule_version), MAX(checked_at)
        FROM audit.marketplace_quality_result WHERE run_id=%s"""

    def __init__(self, connection_factory: Callable[[], Any]): self.connection_factory = connection_factory

    @classmethod
    def from_settings(cls):
        return cls(lambda: psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB))

    def record(self, results: Sequence["QualityResult"], *, run_id: str) -> None:
        if not results: raise ValueError("refusing to record an empty quality result set")
        foreign = sorted({item.run_id for item in results} - {run_id})
        if foreign: raise ValueError(f"quality results belong to another run: {foreign}")
        with self.connection_factory() as conn, conn.cursor() as cur:
            for item in results:
                cur.execute(self._UPSERT, (item.run_id, item.check_name, item.severity, item.dataset_name, item.status,
                                           item.observed_value, item.expectation, item.rule_version,
                                           item.failure_sample_json, item.checked_at))

    def latest_decision(self, *, run_id: str) -> "QualityDecision | None":
        from batch_layer.marketplace_quality import QualityDecision
        from config.quality_rules import mandatory_rule_names
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(self._SUMMARY, (run_id,))
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
        if not row or not row[0]: return None
        mandatory_total, mandatory_failures, advisory_failures, skipped, rule_version, evaluated_at = row
        # A rule with no stored row was never evaluated, which decide() counts
        # as a failure. Reloading must not turn that into a pass.
        expected = len(mandatory_rule_names())
        missing = max(expected - int(mandatory_total), 0)
        failures = int(mandatory_failures) + missing
        return QualityDecision(run_id=run_id, passed=failures == 0, rule_version=rule_version, evaluated_at=evaluated_at,
                               mandatory_total=expected, mandatory_failures=failures,
                               advisory_failures=int(advisory_failures), skipped=int(skipped))


class MarketplaceCachePublisher:
    def __init__(self, connection_factory: Callable[[], Any], jdbc_url: str | None = None, jdbc_properties: Mapping[str, str] | None = None):
        self.connection_factory = connection_factory
        self.jdbc_url = jdbc_url
        self.jdbc_properties = dict(jdbc_properties or {})

    @classmethod
    def from_settings(cls):
        return cls(lambda: psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB), f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}", {"user": POSTGRES_USER, "password": POSTGRES_PASSWORD, "driver": "org.postgresql.Driver"})

    @staticmethod
    def staging_table(dataset_name: str, run_id: str) -> str:
        if dataset_name not in DATASET_COLUMNS: raise ValueError(f"unsupported marketplace dataset: {dataset_name}")
        token = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
        return f"staging.mp_{dataset_name}_{token}"

    def stage(self, marts: Mapping[str, Any], *, run_id: str) -> tuple[StagedDataset, ...]:
        if set(marts) != set(DATASETS): raise ValueError("all ten datasets are required before staging")
        staged = []
        for name in DATASETS:
            frame = marts[name]
            missing = set(DATASET_COLUMNS[name]) - set(frame.columns)
            if missing: raise ValueError(f"{name} missing columns: {sorted(missing)}")
            count = frame.count()
            table = self.staging_table(name, run_id)
            if self.jdbc_url is not None: frame.select(*DATASET_COLUMNS[name]).write.jdbc(self.jdbc_url, table, "overwrite", dict(self.jdbc_properties))
            staged.append(StagedDataset(name, table, count))
        return tuple(staged)

    def publish(self, staged: Sequence[StagedDataset], *, run_id: str, published_at: datetime, quality: "QualityDecision", manifest_uri: str) -> None:
        # The gate, before the advisory lock and before anything is truncated.
        # Orchestration refuses a failed decision earlier; this is the guard
        # that makes an ungated publish impossible rather than merely unlikely,
        # which is why neither argument has a permissive default.
        from batch_layer.marketplace_quality import QualityGateFailure
        if quality.run_id != run_id: raise QualityGateFailure(quality, (), reason=f"quality decision belongs to run {quality.run_id}, not {run_id}")
        if not quality.passed: raise QualityGateFailure(quality, ())
        if not manifest_uri.strip(): raise ValueError("a published cache must name the manifest that admitted it")
        if len(staged) != len(DATASETS) or {x.dataset_name for x in staged} != set(DATASETS): raise ValueError("all ten staged datasets are required")
        for item in staged:
            if item.staging_table != self.staging_table(item.dataset_name, run_id): raise ValueError("staging table does not match run-scoped identity")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("marketplace-cache-publication",))
            counts = {}
            for item in staged:
                if not _IDENT.fullmatch(item.dataset_name): raise ValueError("unsafe dataset identifier")
                cur.execute(f"SELECT COUNT(*) FROM {item.staging_table}")
                actual = cur.fetchone()[0] if hasattr(cur, "fetchone") else item.row_count
                if actual != item.row_count: raise ValueError(f"staging count mismatch for {item.dataset_name}")
                counts[item.dataset_name] = actual
            targets = ", ".join(f"cache.marketplace_{name}" for name in DATASETS)
            cur.execute(f"TRUNCATE {targets}")
            for item in staged:
                columns = ",".join(DATASET_COLUMNS[item.dataset_name])
                cur.execute(f"INSERT INTO cache.marketplace_{item.dataset_name} ({columns}) SELECT {columns} FROM {item.staging_table}")
            # The published cache names the manifest and the rule version that
            # admitted it, so "which Gold version is being served, and under
            # which rules" is answerable from the database alone.
            cur.execute("""INSERT INTO audit.marketplace_cache_version(singleton,run_id,published_at,dataset_counts,quality_rule_version,manifest_uri)
                VALUES(TRUE,%s,%s,%s,%s,%s)
                ON CONFLICT(singleton) DO UPDATE SET run_id=EXCLUDED.run_id,published_at=EXCLUDED.published_at,dataset_counts=EXCLUDED.dataset_counts,
                    quality_rule_version=EXCLUDED.quality_rule_version,manifest_uri=EXCLUDED.manifest_uri""",
                (run_id, published_at, json.dumps(counts, sort_keys=True), quality.rule_version, manifest_uri))
            cur.execute("""UPDATE audit.marketplace_batch_run SET status='SUCCEEDED',completed_at=%s,cache_published=TRUE,
                quality_status='PASS',mandatory_failure_count=0,manifest_uri=%s,manifest_promoted=FALSE WHERE run_id=%s""",
                (published_at, manifest_uri, run_id))

    def cleanup(self, staged: Sequence[StagedDataset]) -> None:
        try:
            with self.connection_factory() as conn, conn.cursor() as cur:
                for item in staged: cur.execute(f"DROP TABLE IF EXISTS {item.staging_table}")
        except Exception:
            pass

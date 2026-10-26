"""Run-scoped staging and atomic PostgreSQL publication for marketplace marts."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ecomdata.domain.batch_run import PROGRESS, BatchRunStatus, check_transition, parse_status
from ecomdata.domain.datasets import MARTS
from ecomdata.processing.batch.context import MarketplaceBatchContext
from ecomdata.processing.batch.ports import RunRecord

if TYPE_CHECKING:
    from ecomdata.processing.batch.quality import QualityDecision, QualityResult

DATASET_COLUMNS = {d.name: list(d.columns) for d in MARTS}
DATASETS = tuple(DATASET_COLUMNS)
_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


@dataclass(frozen=True)
class StagedDataset:
    dataset_name: str
    staging_table: str
    row_count: int


_RUN_COLUMNS = (
    "run_id, as_of, silver_uri, status, last_completed_status, expected_current_run_id, gold_run_uri, silver_rows, "
    "dataset_counts, manifest_uri, quality_status, mandatory_failure_count, cache_published, manifest_promoted, "
    "held_reason, error_message"
)
# The audit columns an ``advance`` or ``set_columns`` call may set.
_SETTABLE = frozenset(
    {
        "gold_run_uri",
        "silver_rows",
        "dataset_counts",
        "manifest_uri",
        "quality_status",
        "mandatory_failure_count",
        "cache_published",
        "manifest_promoted",
        "held_reason",
        "expected_current_run_id",
    }
)
_COMPLETING = frozenset(
    {BatchRunStatus.PROMOTED, BatchRunStatus.QUALITY_FAILED, BatchRunStatus.HELD, BatchRunStatus.FAILED}
)


def _record_from_row(row: Sequence[Any]) -> RunRecord:
    (
        run_id,
        as_of,
        silver_uri,
        status,
        last,
        expected,
        gold_uri,
        silver_rows,
        counts,
        manifest_uri,
        quality_status,
        failures,
        published,
        promoted,
        held_reason,
        error,
    ) = row
    if isinstance(counts, str):
        counts = json.loads(counts)
    return RunRecord(
        run_id=run_id,
        as_of=as_of,
        silver_uri=silver_uri,
        status=parse_status(status),
        last_completed_status=parse_status(last or status),
        expected_current_run_id=expected,
        gold_run_uri=gold_uri,
        silver_rows=silver_rows,
        dataset_counts=counts or {},
        manifest_uri=manifest_uri,
        quality_status=quality_status,
        mandatory_failure_count=failures,
        cache_published=bool(published),
        manifest_promoted=bool(promoted),
        held_reason=held_reason,
        error_message=error,
    )


class MarketplaceBatchAudit:
    """The run's audit row in PostgreSQL; every status change is checked against the domain's state machine."""

    def __init__(self, connection_factory: Callable[[], Any]):
        self.connection_factory = connection_factory

    def get(self, run_id: str) -> RunRecord | None:
        with self.connection_factory() as conn, conn.cursor() as cur:
            return self._select(cur, run_id)

    @staticmethod
    def _select(cur: Any, run_id: str, *, lock: bool = False) -> RunRecord | None:
        cur.execute(
            f"SELECT {_RUN_COLUMNS} FROM audit.marketplace_batch_run WHERE run_id=%s" + (" FOR UPDATE" if lock else ""),
            (run_id,),
        )
        row = cur.fetchone()
        return None if row is None else _record_from_row(row)

    def open_run(
        self, context: MarketplaceBatchContext, now: datetime, *, expected_current_run_id: str | None
    ) -> RunRecord:
        with self.connection_factory() as conn, conn.cursor() as cur:
            existing = self._select(cur, context.run_id, lock=True)
            if existing is not None:
                if existing.as_of != context.as_of or existing.silver_uri != context.silver_uri:
                    raise ValueError("the run exists with another context")
                return existing
            cur.execute(
                """INSERT INTO audit.marketplace_batch_run
                (run_id,as_of,silver_uri,started_at,status,last_completed_status,expected_current_run_id)
                VALUES (%s,%s,%s,%s,'CREATED','CREATED',%s)""",
                (context.run_id, context.as_of, context.silver_uri, now, expected_current_run_id),
            )
        return RunRecord(
            context.run_id,
            context.as_of,
            context.silver_uri,
            BatchRunStatus.CREATED,
            BatchRunStatus.CREATED,
            expected_current_run_id=expected_current_run_id,
        )

    def advance(self, run_id: str, target: BatchRunStatus, now: datetime, **columns: Any) -> RunRecord:
        columns.setdefault("held_reason", None)
        with self.connection_factory() as conn, conn.cursor() as cur:
            current = self._select(cur, run_id, lock=True)
            if current is None:
                raise KeyError(run_id)
            check_transition(current.status, target)
            assignments = ["status=%s", "error_message=NULL", "completed_at=%s"]
            values: list[Any] = [target.value, now if target in _COMPLETING else None]
            if target in PROGRESS:
                assignments.append("last_completed_status=%s")
                values.append(target.value)
            self._assign(columns, assignments, values)
            cur.execute(
                f"UPDATE audit.marketplace_batch_run SET {', '.join(assignments)} WHERE run_id=%s", (*values, run_id)
            )
            record = self._select(cur, run_id)
        if record is None:
            raise RuntimeError("the run row vanished after its update")
        return record

    def reopen(self, run_id: str, now: datetime) -> RunRecord:
        with self.connection_factory() as conn, conn.cursor() as cur:
            current = self._select(cur, run_id, lock=True)
            if current is None:
                raise KeyError(run_id)
            if current.status in (BatchRunStatus.FAILED, BatchRunStatus.QUALITY_FAILED):
                cur.execute(
                    "UPDATE audit.marketplace_batch_run SET status=last_completed_status,error_message=NULL,"
                    "completed_at=NULL WHERE run_id=%s",
                    (run_id,),
                )
                current = self._select(cur, run_id)
        if current is None:
            raise RuntimeError("the run row vanished after its update")
        return current

    def fail(self, run_id: str, now: datetime, error: BaseException) -> None:
        with self.connection_factory() as conn, conn.cursor() as cur:
            current = self._select(cur, run_id, lock=True)
            if current is None:
                raise KeyError(run_id)
            check_transition(current.status, BatchRunStatus.FAILED)
            cur.execute(
                "UPDATE audit.marketplace_batch_run SET status='FAILED',completed_at=%s,error_message=%s WHERE run_id=%s",
                (now, str(error)[:2000], run_id),
            )

    def set_columns(self, run_id: str, **columns: Any) -> None:
        if not columns:
            return
        assignments: list[str] = []
        values: list[Any] = []
        self._assign(columns, assignments, values)
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE audit.marketplace_batch_run SET {', '.join(assignments)} WHERE run_id=%s", (*values, run_id)
            )

    @staticmethod
    def _assign(columns: Mapping[str, Any], assignments: list[str], values: list[Any]) -> None:
        for name, value in columns.items():
            if name not in _SETTABLE:
                raise ValueError(f"unknown audit column: {name}")
            if name == "dataset_counts":
                assignments += ["dataset_counts=%s", "gold_rows=%s"]
                values += [json.dumps(dict(value), sort_keys=True), sum(value.values())]
            else:
                assignments.append(f"{name}=%s")
                values.append(value)


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

    def __init__(self, connection_factory: Callable[[], Any]):
        self.connection_factory = connection_factory

    def record(self, results: Sequence[QualityResult], *, run_id: str) -> None:
        if not results:
            raise ValueError("refusing to record an empty quality result set")
        foreign = sorted({item.run_id for item in results} - {run_id})
        if foreign:
            raise ValueError(f"quality results belong to another run: {foreign}")
        with self.connection_factory() as conn, conn.cursor() as cur:
            for item in results:
                cur.execute(
                    self._UPSERT,
                    (
                        item.run_id,
                        item.check_name,
                        item.severity,
                        item.dataset_name,
                        item.status,
                        item.observed_value,
                        item.expectation,
                        item.rule_version,
                        item.failure_sample_json,
                        item.checked_at,
                    ),
                )

    def latest_decision(self, *, run_id: str) -> QualityDecision | None:
        from ecomdata.domain.quality_rules import mandatory_rule_names
        from ecomdata.processing.batch.quality import QualityDecision

        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(self._SUMMARY, (run_id,))
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
        if not row or not row[0]:
            return None
        mandatory_total, mandatory_failures, advisory_failures, skipped, rule_version, evaluated_at = row
        # A rule with no stored row was never evaluated, which decide() counts
        # as a failure. Reloading must not turn that into a pass.
        expected = len(mandatory_rule_names())
        missing = max(expected - int(mandatory_total), 0)
        failures = int(mandatory_failures) + missing
        return QualityDecision(
            run_id=run_id,
            passed=failures == 0,
            rule_version=rule_version,
            evaluated_at=evaluated_at,
            mandatory_total=expected,
            mandatory_failures=failures,
            advisory_failures=int(advisory_failures),
            skipped=int(skipped),
        )


class MarketplaceCachePublisher:
    def __init__(
        self,
        connection_factory: Callable[[], Any],
        jdbc_url: str | None = None,
        jdbc_properties: Mapping[str, str] | None = None,
    ):
        self.connection_factory = connection_factory
        self.jdbc_url = jdbc_url
        self.jdbc_properties = dict(jdbc_properties or {})

    @staticmethod
    def staging_table(dataset_name: str, run_id: str) -> str:
        if dataset_name not in DATASET_COLUMNS:
            raise ValueError(f"unsupported marketplace dataset: {dataset_name}")
        token = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
        return f"staging.mp_{dataset_name}_{token}"

    def stage(self, marts: Mapping[str, Any], *, run_id: str) -> tuple[StagedDataset, ...]:
        if set(marts) != set(DATASETS):
            raise ValueError("all ten datasets are required before staging")
        staged = []
        for name in DATASETS:
            frame = marts[name]
            missing = set(DATASET_COLUMNS[name]) - set(frame.columns)
            if missing:
                raise ValueError(f"{name} missing columns: {sorted(missing)}")
            count = frame.count()
            table = self.staging_table(name, run_id)
            if self.jdbc_url is not None:
                frame.select(*DATASET_COLUMNS[name]).write.jdbc(
                    self.jdbc_url, table, "overwrite", dict(self.jdbc_properties)
                )
            staged.append(StagedDataset(name, table, count))
        return tuple(staged)

    def publish(
        self,
        staged: Sequence[StagedDataset],
        *,
        run_id: str,
        published_at: datetime,
        quality: QualityDecision,
        manifest_uri: str,
    ) -> None:
        # The gate, before the advisory lock and before anything is truncated.
        # Orchestration refuses a failed decision earlier; this is the guard
        # that makes an ungated publish impossible rather than merely unlikely,
        # which is why neither argument has a permissive default.
        from ecomdata.processing.batch.quality import QualityGateFailure

        if quality.run_id != run_id:
            raise QualityGateFailure(
                quality, (), reason=f"quality decision belongs to run {quality.run_id}, not {run_id}"
            )
        if not quality.passed:
            raise QualityGateFailure(quality, ())
        if not manifest_uri.strip():
            raise ValueError("a published cache must name the manifest that admitted it")
        if len(staged) != len(DATASETS) or {x.dataset_name for x in staged} != set(DATASETS):
            raise ValueError("all ten staged datasets are required")
        for item in staged:
            if item.staging_table != self.staging_table(item.dataset_name, run_id):
                raise ValueError("staging table does not match run-scoped identity")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("marketplace-cache-publication",))
            counts = {}
            for item in staged:
                if not _IDENT.fullmatch(item.dataset_name):
                    raise ValueError("unsafe dataset identifier")
                cur.execute(f"SELECT COUNT(*) FROM {item.staging_table}")
                actual = cur.fetchone()[0] if hasattr(cur, "fetchone") else item.row_count
                if actual != item.row_count:
                    raise ValueError(f"staging count mismatch for {item.dataset_name}")
                counts[item.dataset_name] = actual
            targets = ", ".join(f"cache.marketplace_{name}" for name in DATASETS)
            cur.execute(f"TRUNCATE {targets}")
            for item in staged:
                columns = ",".join(DATASET_COLUMNS[item.dataset_name])
                cur.execute(
                    f"INSERT INTO cache.marketplace_{item.dataset_name} ({columns}) SELECT {columns} FROM {item.staging_table}"
                )
            # The published cache names the manifest and the rule version that
            # admitted it, so "which Gold version is being served, and under
            # which rules" is answerable from the database alone.
            cur.execute(
                """INSERT INTO audit.marketplace_cache_version(singleton,run_id,published_at,dataset_counts,quality_rule_version,manifest_uri)
                VALUES(TRUE,%s,%s,%s,%s,%s)
                ON CONFLICT(singleton) DO UPDATE SET run_id=EXCLUDED.run_id,published_at=EXCLUDED.published_at,dataset_counts=EXCLUDED.dataset_counts,
                    quality_rule_version=EXCLUDED.quality_rule_version,manifest_uri=EXCLUDED.manifest_uri""",
                (run_id, published_at, json.dumps(counts, sort_keys=True), quality.rule_version, manifest_uri),
            )

    def cleanup(self, staged: Sequence[StagedDataset]) -> None:
        try:
            with self.connection_factory() as conn, conn.cursor() as cur:
                for item in staged:
                    cur.execute(f"DROP TABLE IF EXISTS {item.staging_table}")
        except Exception:
            pass

"""What a batch stage talks to, as Protocols.

Stage logic receives these and nothing else: the batch bootstrap injects the
PostgreSQL, object-store and Spark implementations, and a test injects the
in-memory ones (``processing/batch/memory.py``).
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from ecomdata.domain.batch_run import BatchRunStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from ecomdata.processing.batch.context import DwWriteResult, GoldWriteResult, MarketplaceBatchContext
    from ecomdata.processing.batch.quality import QualityDecision, QualityResult
    from ecomdata.processing.batch.repositories import StagedDataset


@dataclass(frozen=True)
class RunRecord:
    """One batch run as the audit holds it."""

    run_id: str
    as_of: datetime
    silver_uri: str
    status: BatchRunStatus
    # The furthest status the run reached on the happy path; a FAILED run resumes from it.
    last_completed_status: BatchRunStatus
    # The serving pointer's run when this run was created: the compare-and-swap token of its promotion.
    expected_current_run_id: str | None = None
    gold_run_uri: str | None = None
    silver_rows: int | None = None
    dataset_counts: Mapping[str, int] = field(default_factory=dict)
    manifest_uri: str | None = None
    quality_status: str | None = None
    mandatory_failure_count: int | None = None
    cache_published: bool = False
    manifest_promoted: bool = False
    held_reason: str | None = None
    error_message: str | None = None

    @property
    def resume_from(self) -> BatchRunStatus:
        """Where a continued run picks up: a failed run goes back to its last good status."""
        return self.last_completed_status if self.status is BatchRunStatus.FAILED else self.status


class ObjectStore(Protocol):
    def put(self, zone: str, key: str, data: bytes) -> str: ...

    def get(self, zone: str, key: str) -> bytes | None: ...


class AuditRepo(Protocol):
    """The run's audit row; every status change goes through the domain's state machine."""

    def get(self, run_id: str) -> RunRecord | None: ...

    def open_run(
        self,
        context: MarketplaceBatchContext,
        now: datetime,
        *,
        expected_current_run_id: str | None,
    ) -> RunRecord:
        """The run's row, created as CREATED if it does not exist; refuse a row of another context."""

    def advance(self, run_id: str, target: BatchRunStatus, now: datetime, **columns: Any) -> RunRecord:
        """Move the run to ``target`` (refusing a transition the machine forbids) and set ``columns``."""

    def reopen(self, run_id: str, now: datetime) -> RunRecord:
        """Put a FAILED run back to the status it last completed, clearing its error."""

    def fail(self, run_id: str, now: datetime, error: BaseException) -> None: ...

    def set_columns(self, run_id: str, **columns: Any) -> None:
        """Set audit columns that do not change the status (a promotion flag)."""


class QualityRepo(Protocol):
    def record(self, results: Sequence[QualityResult], *, run_id: str) -> None: ...

    def latest_decision(self, *, run_id: str) -> QualityDecision | None: ...


class CachePublisher(Protocol):
    def stage(self, marts: Mapping[str, DataFrame], *, run_id: str) -> Sequence[StagedDataset]: ...

    def publish(
        self,
        staged: Sequence[StagedDataset],
        *,
        run_id: str,
        published_at: datetime,
        quality: QualityDecision,
        manifest_uri: str,
    ) -> None: ...

    def cleanup(self, staged: Sequence[StagedDataset]) -> None: ...


class LakeIO(Protocol):
    """Reading and writing the datasets stages hand to each other through storage."""

    def read_records(self, *, since_date: Any = None) -> DataFrame:
        """Bronze/records (envelope columns and ``payload_json``), from ``since_date`` on."""

    def write_silver(self, name: str, frame: DataFrame, *, partition_by: Sequence[str] = ()) -> str: ...

    def read_silver(self, name: str) -> DataFrame: ...

    def write_gold(self, context: MarketplaceBatchContext, name: str, frame: DataFrame) -> GoldWriteResult: ...

    def read_gold(self, context: MarketplaceBatchContext, name: str) -> DataFrame: ...

    def merge_dw(self, context: MarketplaceBatchContext, name: str, frame: DataFrame) -> DwWriteResult:
        """Make the star table ``name`` hold exactly ``frame``, matched on the registry key.

        Delta: one ``MERGE`` (insert new keys, update changed rows, delete keys
        ``frame`` no longer has), so a rerun on the same data rewrites no row
        and every earlier version stays readable. Parquet: an overwrite.
        """

    def read_dw(self, context: MarketplaceBatchContext, name: str) -> DataFrame: ...


BatchLock = Callable[[], AbstractContextManager[Any]]
SparkProvider = Callable[[], "SparkSession"]
CrawlAuditReader = Callable[["SparkSession"], "tuple[DataFrame, DataFrame]"]

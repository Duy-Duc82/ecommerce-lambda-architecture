"""The Stage protocol and what every stage is handed.

A stage declares its name, the artifacts it ``requires`` and ``produces``, and
the statuses of a run it may start from (``allowed_from``). It reads its inputs
from storage and writes its outputs to storage, never passes a DataFrame to the
next stage, and is idempotent: running it again on a run that already passed its
milestone is a no-op (the runner enforces that), and running it again with
``force`` rewrites the same outputs from the same inputs.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from ecomdata.domain.batch_run import BatchRunStatus
from ecomdata.processing.batch.context import MarketplaceBatchContext
from ecomdata.processing.batch.ports import (
    AuditRepo,
    CachePublisher,
    CrawlAuditReader,
    LakeIO,
    ObjectStore,
    QualityRepo,
    RunRecord,
    SparkProvider,
)


class StageNotAllowed(RuntimeError):
    """The run is in a status the stage cannot start from."""

    def __init__(self, stage: str, status: BatchRunStatus, allowed: frozenset[BatchRunStatus]):
        names = ", ".join(sorted(item.value for item in allowed))
        super().__init__(f"stage {stage} cannot run on a {status.value} run (allowed from: {names})")
        self.stage, self.status, self.allowed = stage, status, allowed


@dataclass(frozen=True)
class RunOptions:
    # Let an older as_of become the serving version (a deliberate operator command).
    allow_backfill: bool = False
    # Judge and write the run's Gold and manifest, then hold it off the serving version.
    quality_only: bool = False
    # False: no PostgreSQL cache (the audit and the cache publisher are skipped).
    publish_cache: bool = True
    # The Gold root the manifests and the pointer describe; a run into another root is a scratch run.
    serving_gold_root_uri: str | None = None
    # Silver rebuilds the day partitions from this date on (None: every record in Bronze).
    silver_since: date | None = None
    # Run a stage again although the run already passed its milestone.
    force: bool = False

    def is_scratch(self, context: MarketplaceBatchContext) -> bool:
        root = self.serving_gold_root_uri
        return root is not None and context.gold_root_uri.rstrip("/") != root.rstrip("/")


@dataclass
class StageContext:
    run: MarketplaceBatchContext
    record: RunRecord
    options: RunOptions
    audit: AuditRepo
    store: ObjectStore
    io: LakeIO
    quality: QualityRepo
    spark_provider: SparkProvider
    crawl_audit: CrawlAuditReader
    publisher: CachePublisher | None = None
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    _spark: Any = field(default=None, repr=False)

    @property
    def spark(self) -> Any:
        if self._spark is None:
            self._spark = self.spark_provider()
        return self._spark

    def now(self) -> datetime:
        return self.clock()


@dataclass(frozen=True)
class StageResult:
    stage: str
    status: BatchRunStatus
    detail: Mapping[str, Any] = field(default_factory=dict)
    skipped: bool = False


@dataclass(frozen=True)
class Outcome:
    """What a stage's work decided: the status to move to and the audit columns to set."""

    status: BatchRunStatus
    columns: Mapping[str, Any] = field(default_factory=dict)
    detail: Mapping[str, Any] = field(default_factory=dict)
    # Statuses the run passes through, in order, before ``status`` (a held run first passes its quality gate).
    via: tuple[BatchRunStatus, ...] = ()
    # Raised by the runner once the status and columns are recorded: a refused run keeps its evidence.
    failure: BaseException | None = None


class Stage(ABC):
    name: str
    requires: tuple[str, ...] = ()
    produces: tuple[str, ...] = ()
    allowed_from: frozenset[BatchRunStatus] = frozenset()
    # The status a run has reached once this stage is done on the happy path.
    milestone: BatchRunStatus

    @abstractmethod
    def execute(self, ctx: StageContext) -> Outcome:
        """Do the stage's work from storage to storage, and say where the run goes next."""

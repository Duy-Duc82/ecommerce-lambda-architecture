"""Stage ``silver``: Bronze records to canonical observations, per source.

A run owns a **window of days** (:func:`silver_days`): the day of its
``as_of`` and the ``late_days`` before it, or every day from ``silver_since``,
or, for ``--rebuild``, every day Bronze holds. It reads every Bronze record of
those days, whatever its time against ``as_of`` (``as_of`` cuts when a later
stage reads Silver, never when Silver is written, so an older run can never
shrink a day a newer run wrote), runs each source's normaliser
(``SourceRegistry.normalizer``), and replaces exactly those day partitions of
every Silver table; a day outside the window is never touched. All tables are
partitioned by ``source_code`` and ``observed_date``, the date of
``fetched_at``, which is also the Bronze partition a record was written to (a
record without a readable ``fetched_at`` keeps that partition's date). It writes:

* ``offer_observations_v2``: the valid canonical rows, sponsored ones marked;
* ``quarantine``: every row that could not be an observation, with its reason
  (a missing id or price, a repeated listing on one page, an envelope this code
  does not read), so no record is lost between Bronze and Silver;
* ``<source>_listing_wide``: the source's rows with every declared payload field
  typed, plus the raw ``payload_json``;
* ``page_context``: the page around the rows;
* a run summary under ``_runs``: the counts, the payload drift report, and the
  duplicates: ``record_id`` (a Bronze line written twice, a replay, which is
  dropped so the stage stays idempotent) and, over the valid rows, a repeated
  ``observation_id`` and a repeated whole row. Those two are counted before
  anything deduplicates them and no row is dropped for them: they are what the
  quality rule ``observation_id_unique`` reads.
"""
from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from functools import partial
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.sources import SOURCES, SourceRegistry
from ecomdata.processing.batch.inputs import duplicate_counts
from ecomdata.processing.batch.stages.base import Outcome, RunOptions, Stage, StageContext
from ecomdata.processing.normalize import common

OBSERVATIONS = "offer_observations_v2"
QUARANTINE = "quarantine"
PAGE_CONTEXT = "page_context"
RUNS_DIRECTORY = "_runs"
SILVER_ZONE = "silver"
SILVER_PARTITIONS = ("source_code", "observed_date")
QUARANTINE_COLUMNS = (
    "record_id",
    "source_code",
    "raw_artifact_id",
    "crawl_run_id",
    "request_target",
    "request_page",
    "fetched_at",
    "reason",
    "payload_json",
    "observed_date",
)


def silver_days(as_of: datetime, options: RunOptions) -> tuple[date, ...] | None:
    """The days a run rebuilds, oldest first; ``None`` for every day Bronze holds (``--rebuild``)."""
    if options.rebuild:
        return None
    last = as_of.astimezone(UTC).date()
    first = options.silver_since if options.silver_since is not None else last - timedelta(days=options.late_days)
    if first > last:
        raise ValueError(f"silver_since {first} is after the run's day {last}")
    return tuple(first + timedelta(days=n) for n in range((last - first).days + 1))


def record_day():
    """The Silver day of a record: the date of ``fetched_at``, else its Bronze partition's date."""
    return F.coalesce(F.to_date("fetched_at"), F.col("bronze_date"))


def wide_name(source_code: str) -> str:
    return f"{source_code}_listing_wide"


def summary_key(run_id: str) -> str:
    return f"marketplace/{RUNS_DIRECTORY}/run_id={run_id}/silver.json"


class SilverStage(Stage):
    name = "silver"
    requires = ("bronze/marketplace/records",)
    produces = (
        f"silver/{OBSERVATIONS}",
        f"silver/{QUARANTINE}",
        f"silver/{PAGE_CONTEXT}",
        "silver/<source>_listing_wide",
        "silver/_runs/<run>/silver.json",
    )
    allowed_from = frozenset({S.CREATED})
    milestone = S.SILVER_BUILT

    def __init__(self, registry: SourceRegistry = SOURCES) -> None:
        self.registry = registry

    def execute(self, ctx: StageContext) -> Outcome:
        run, options = ctx.run, ctx.options
        days = silver_days(run.as_of, options)
        records = ctx.io.read_records(days=days)
        if days is not None:
            # The lake reads only these days' Bronze partitions; the filter keeps
            # any reader to the window.
            records = records.filter(record_day().isin(*days))
        records, replayed_records = drop_replayed_records(records)

        valid_frames: list[DataFrame] = []
        quarantine_frames: list[DataFrame] = []
        wide_frames: dict[str, DataFrame] = {}
        drift: list[dict[str, Any]] = []
        for code in self._sources_in(records):
            normalizer = self.registry.normalizer(code)
            rows = common.listing_rows(records.filter(F.col("source_code") == code))
            valid, quarantined = common.split_quarantine(normalizer.to_offer_observation(rows))
            valid_frames.append(valid)
            quarantine_frames.append(with_record_day(quarantined))
            wide_frames[code] = with_record_day(normalizer.to_wide(records.filter(F.col("source_code") == code)))
            drift += [row.asDict() for row in common.payload_drift(rows, normalizer.PAYLOAD_SCHEMA).collect()]

        unsupported = common.unsupported_envelopes(records)
        quarantine_frames.append(with_record_day(self._unsupported_rows(unsupported)))
        known_codes = [spec.code for spec in self.registry]
        unknown = records.filter(~F.col("source_code").isin(*known_codes) | F.col("source_code").isNull())
        quarantine_frames.append(with_record_day(self._unknown_source_rows(common.listing_rows(unknown))))

        observations = _union(valid_frames, None)
        quarantine = _union(quarantine_frames, QUARANTINE_COLUMNS)
        if observations is None:
            observations = ctx.spark.createDataFrame([], schema=_canonical_schema(ctx.spark))
        silver_rows = 0
        duplicates = {"rows": 0, "observation_id": 0, "identical_content": 0}
        observations = observations.cache()
        quarantine = quarantine.cache()
        try:
            if valid_frames:
                duplicates = duplicate_counts(observations)
                silver_rows = duplicates["rows"]
            write = partial(ctx.io.write_silver, partition_by=SILVER_PARTITIONS, whole=days is None)
            write(OBSERVATIONS, observations)
            quarantine_by_reason = {
                row.reason: row.n for row in quarantine.groupBy("reason").agg(F.count("*").alias("n")).collect()
            }
            write(QUARANTINE, quarantine)
        finally:
            observations.unpersist()
            quarantine.unpersist()
        for code, wide in wide_frames.items():
            write(wide_name(code), wide.drop("dropped_fields_json"))
        contexts = with_record_day(common.page_contexts(records))
        write(PAGE_CONTEXT, contexts)

        summary = {
            "run_id": run.run_id,
            "as_of": run.as_of.isoformat(),
            "silver_rows": silver_rows,
            "quarantined": sum(quarantine_by_reason.values()),
            "quarantine_by_reason": dict(sorted(quarantine_by_reason.items())),
            "page_contexts": contexts.count(),
            "duplicates_before_dedup": {
                "record_id": replayed_records,
                **{k: v for k, v in duplicates.items() if k != "rows"},
            },
            "payload_drift": drift,
            "window": None if days is None else [day.isoformat() for day in days],
            "rebuild": days is None,
        }
        ctx.store.put(SILVER_ZONE, summary_key(run.run_id), json.dumps(summary, sort_keys=True).encode("utf-8"))
        return Outcome(S.SILVER_BUILT, columns={"silver_rows": silver_rows}, detail=summary)

    def _sources_in(self, records: DataFrame) -> list[str]:
        present = {row.source_code for row in records.select("source_code").distinct().collect()}
        return [spec.code for spec in self.registry if spec.code in present]

    @staticmethod
    def _unsupported_rows(unsupported: DataFrame) -> DataFrame:
        return unsupported.select(
            F.col("record_id"),
            F.col("source_code"),
            F.col("raw_artifact_id"),
            F.col("crawl_run_id"),
            F.col("request_target"),
            F.col("request_page"),
            F.col("fetched_at"),
            F.lit("unsupported_envelope").alias("reason"),
            F.col("payload_json"),
            F.col("bronze_date"),
        )

    @staticmethod
    def _unknown_source_rows(rows: DataFrame) -> DataFrame:
        return rows.select(
            F.col("record_id"),
            F.col("source_code"),
            F.col("raw_artifact_id"),
            F.col("crawl_run_id"),
            F.col("request_target"),
            F.col("request_page"),
            F.col("fetched_at"),
            F.lit("unknown_source").alias("reason"),
            F.col("payload_json"),
            F.col("bronze_date"),
        )


def with_record_day(frame: DataFrame) -> DataFrame:
    """``frame`` with the day it is partitioned by (``observed_date``) in place of ``bronze_date``."""
    return frame.withColumn("observed_date", record_day()).drop("bronze_date")


def drop_replayed_records(records: DataFrame) -> tuple[DataFrame, int]:
    """The records with each ``record_id`` once, and how many extra copies there were.

    A record's id is deterministic from its artifact, kind and position, so a
    second copy is the same line written again (a retried Bronze write), never a
    different observation. A row without an id is kept: it goes to quarantine.
    """
    counts = records.agg(F.count("record_id").alias("with_id"), F.countDistinct("record_id").alias("ids")).first()
    replayed = counts.with_id - counts.ids
    if not replayed:
        return records, 0
    keyed = records.filter(F.col("record_id").isNotNull()).dropDuplicates(["record_id"])
    return keyed.unionByName(records.filter(F.col("record_id").isNull())), replayed


def _union(frames: list[DataFrame], columns: tuple[str, ...] | None) -> DataFrame | None:
    if not frames:
        return None
    result = frames[0] if columns is None else frames[0].select(*columns)
    for frame in frames[1:]:
        result = result.unionByName(frame if columns is None else frame.select(*columns))
    return result


def _canonical_schema(spark: Any):
    return spark.createDataFrame(
        [], ", ".join(f"{name} {dtype}" for name, dtype in common.CANONICAL_COLUMNS)
    ).schema

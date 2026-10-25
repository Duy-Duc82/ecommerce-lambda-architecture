"""Stage ``silver``: Bronze records to canonical observations, per source.

Reads Bronze/records up to the run's ``as_of``, runs each source's normaliser
(``SourceRegistry.normalizer``), and writes, all to storage:

* ``offer_observations_v2``: the valid canonical rows, sponsored ones marked,
  partitioned by ``source_code`` and ``observed_date``. The day partitions from
  ``silver_since`` on are replaced whole, so a rerun gives the same files;
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
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecomdata.domain.batch_run import BatchRunStatus as S
from ecomdata.domain.sources import SOURCES, SourceRegistry
from ecomdata.processing.batch.stages.base import Outcome, Stage, StageContext
from ecomdata.processing.normalize import common

OBSERVATIONS = "offer_observations_v2"
QUARANTINE = "quarantine"
PAGE_CONTEXT = "page_context"
RUNS_DIRECTORY = "_runs"
SILVER_ZONE = "silver"
SILVER_PARTITIONS = ("source_code", "observed_date")
# Which record a row came from, not what the row says.
LINEAGE_COLUMNS = ("record_id", "raw_artifact_id")
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
)


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
        records = ctx.io.read_records(since_date=options.silver_since).filter(F.col("fetched_at") <= F.lit(run.as_of))
        records, replayed_records = drop_replayed_records(records)
        since = F.lit(options.silver_since) if options.silver_since else None

        valid_frames: list[DataFrame] = []
        quarantine_frames: list[DataFrame] = []
        wide_frames: dict[str, DataFrame] = {}
        drift: list[dict[str, Any]] = []
        for code in self._sources_in(records):
            normalizer = self.registry.normalizer(code)
            rows = common.listing_rows(records.filter(F.col("source_code") == code))
            valid, quarantined = common.split_quarantine(normalizer.to_offer_observation(rows))
            if since is not None:
                valid = valid.filter(F.col("observed_date") >= since)
            valid_frames.append(valid)
            quarantine_frames.append(quarantined)
            wide_frames[code] = normalizer.to_wide(records.filter(F.col("source_code") == code))
            drift += [row.asDict() for row in common.payload_drift(rows, normalizer.PAYLOAD_SCHEMA).collect()]

        unsupported = common.unsupported_envelopes(records)
        quarantine_frames.append(self._unsupported_rows(unsupported))
        known_codes = [spec.code for spec in self.registry]
        unknown = records.filter(~F.col("source_code").isin(*known_codes) | F.col("source_code").isNull())
        quarantine_frames.append(self._unknown_source_rows(common.listing_rows(unknown)))

        observations = _union(valid_frames, None)
        quarantine = _union(quarantine_frames, QUARANTINE_COLUMNS)
        if observations is None:
            observations = ctx.spark.createDataFrame([], schema=_canonical_schema(ctx.spark))
        silver_rows = 0
        duplicates = {"rows": 0, "observation_id": 0, "identical_content": 0}
        if valid_frames:
            observations = observations.cache()
            duplicates = duplicate_counts(observations)
            silver_rows = duplicates["rows"]
        ctx.io.write_silver(OBSERVATIONS, observations, partition_by=SILVER_PARTITIONS)
        quarantine = quarantine.cache()
        quarantine_by_reason = {row.reason: row.n for row in quarantine.groupBy("reason").agg(F.count("*").alias("n")).collect()}
        ctx.io.write_silver(QUARANTINE, quarantine, partition_by=("source_code",))
        for code, wide in wide_frames.items():
            ctx.io.write_silver(wide_name(code), wide.drop("dropped_fields_json"), partition_by=())
        contexts = common.page_contexts(records)
        ctx.io.write_silver(PAGE_CONTEXT, contexts, partition_by=("source_code",))

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
            "silver_since": options.silver_since.isoformat() if options.silver_since else None,
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
        )


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


def duplicate_counts(observations: DataFrame) -> dict[str, int]:
    """Rows, and how many of them repeat an ``observation_id`` or repeat a whole row, before any dedup.

    ``observation_id`` is deterministic from (source, listing, instant, body), so a
    repeat means the same listing twice in one response (or a replayed record).
    ``identical_content`` counts the repeats whose canonical columns, bar the
    lineage ones (``LINEAGE_COLUMNS``, which name the record the row came from), equal
    another row's; ``observation_id`` minus ``identical_content`` is the number
    of ids that carry different content, which dedup would refuse.
    """
    content = F.sha2(
        F.to_json(F.struct(*[F.col(name) for name in observations.columns if name not in LINEAGE_COLUMNS])), 256
    )
    row = observations.agg(
        F.count("*").alias("rows"),
        F.countDistinct("observation_id").alias("ids"),
        F.countDistinct(content).alias("contents"),
    ).first()
    return {
        "rows": row.rows,
        "observation_id": row.rows - row.ids,
        "identical_content": row.rows - row.contents,
    }


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

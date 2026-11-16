"""The speed layer on a lakehouse: Bronze records to Delta tables, without Kafka, Elasticsearch or Redis.

``service`` runs the speed layer next to the local Kafka. A platform that cannot reach that Kafka (a
Databricks job) runs this module instead. The two queries keep their transforms (the same normaliser,
the same stateful comparison and change rules); only their ends change:

    Bronze/records (JSONL.gz)  --normaliser-->  <root>/observations (Delta)   rejected rows: <root>/records_dlq
    <root>/observations  --stateful comparison-->  <root>/outputs        every state, change, duplicate, late, invalid row
                                                   <root>/offer_state    the latest state of each offer (Redis's role)
                                                   <root>/price_changes  one row per change (Elasticsearch's role)
    Spark's progress of each micro-batch  ------>  <root>/stream_progress

A micro-batch replayed after a failure writes nothing twice. Appends carry Delta's ``txnAppId`` and
``txnVersion`` (the query id kept in the checkpoint, and the batch id); ``offer_state`` only moves forward;
``price_changes`` inserts a change id once.

    python -m ecomdata.processing.speed.lakehouse [--available-now] [--run-minutes N] [--auto-loader]
"""
from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecomdata.processing.speed.normalizer import normalize_micro_batch

# Lineage of an observation read back from the Delta table, in the columns the Kafka source fills.
OBSERVATIONS_SOURCE = "lakehouse.observations"

PROGRESS_SCHEMA = (
    "query_name string, query_id string, batch_id long, run_id string, progress_at timestamp, "
    "recorded_at timestamp, num_input_rows long, input_rows_per_second double, "
    "processed_rows_per_second double, batch_duration_ms long, trigger_execution_ms long, add_batch_ms long, "
    "get_batch_ms long, latest_offset_ms long, query_planning_ms long, wal_commit_ms long, "
    "commit_offsets_ms long, state_rows_total long, state_memory_bytes long, offsets_behind_latest long"
)


@dataclass(frozen=True)
class LakehousePaths:
    """Where the speed tables and checkpoints live, under one root URI."""

    root: str

    def table(self, name: str) -> str:
        return f"{self.root.rstrip('/')}/{name}"

    @property
    def observations(self) -> str:
        return self.table("observations")

    @property
    def records_dlq(self) -> str:
        return self.table("records_dlq")

    @property
    def outputs(self) -> str:
        return self.table("outputs")

    @property
    def offer_state(self) -> str:
        return self.table("offer_state")

    @property
    def price_changes(self) -> str:
        return self.table("price_changes")

    @property
    def stream_progress(self) -> str:
        return self.table("stream_progress")

    def checkpoint(self, query: str) -> str:
        return self.table(f"_checkpoints/{query}")


# -- SQL as text, testable without Spark -------------------------------------------------------------


def offer_state_merge(uri: str, source: str) -> str:
    """Upsert each offer's latest state; an older state (a replayed or late batch) never overwrites a newer one."""
    newer = "s.event_time > t.event_time OR (s.event_time = t.event_time AND s.observation_id > t.observation_id)"
    return (
        f"MERGE INTO delta.`{uri}` AS t USING {source} AS s ON t.offer_id = s.offer_id "
        f"WHEN MATCHED AND ({newer}) THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def insert_once(uri: str, source: str, key: str) -> str:
    """Insert the rows whose ``key`` the table does not hold yet; a row already there is left alone."""
    return f"MERGE INTO delta.`{uri}` AS t USING {source} AS s ON t.`{key}` = s.`{key}` WHEN NOT MATCHED THEN INSERT *"


# -- Delta helpers -----------------------------------------------------------------------------------


def _is_delta(spark: SparkSession, uri: str) -> bool:
    from pyspark.errors import AnalysisException

    try:
        spark.read.format("delta").load(uri).limit(0).collect()
        return True
    except AnalysisException:
        return False


def idempotent_append(frame: DataFrame, uri: str, *, app_id: str, version: int) -> None:
    """Append once per (app_id, version): Delta skips a write whose version it has already committed."""
    (
        frame.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", version)
        .option("mergeSchema", "true")
        .save(uri)
    )


def merge_or_create(frame: DataFrame, uri: str, statement: Callable[[str, str], str], view: str) -> None:
    """Create the table from ``frame`` on its first write, MERGE into it afterwards."""
    spark = frame.sparkSession
    if not _is_delta(spark, uri):
        frame.write.format("delta").mode("append").save(uri)
        return
    frame.createOrReplaceTempView(view)
    spark.sql(statement(uri, view))


def _query_id(batch: DataFrame) -> str:
    query_id = batch.sparkSession.sparkContext.getLocalProperty("sql.streaming.queryId")
    if not query_id:
        raise ValueError("micro-batch carries no streaming query id; refusing an append that could not be deduplicated")
    return query_id


# -- Query 1: Bronze record files -> observation events ------------------------------------------------


def read_record_files(
    spark: SparkSession, records_uri: str, *, auto_loader: bool = False, max_files_per_trigger: int = 0
) -> DataFrame:
    """Each line of the Bronze record files, as a ``value`` column, as the files arrive.

    ``auto_loader`` uses Databricks Auto Loader (``cloudFiles``), which lists a bucket incrementally;
    the plain file source works everywhere but lists the whole prefix every trigger.
    """
    if auto_loader:
        reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", "text")
        if max_files_per_trigger > 0:
            reader = reader.option("cloudFiles.maxFilesPerTrigger", str(max_files_per_trigger))
    else:
        reader = spark.readStream.format("text").option("recursiveFileLookup", "true")
        if max_files_per_trigger > 0:
            reader = reader.option("maxFilesPerTrigger", str(max_files_per_trigger))
    return reader.load(records_uri).select("value")


def normalizer_to_delta(paths: LakehousePaths) -> Callable[[DataFrame, int], None]:
    """``foreachBatch`` function: normalise a micro-batch of record lines into the observation table."""

    def write(batch: DataFrame, batch_id: int) -> None:
        app_id = _query_id(batch)
        events, rejected = normalize_micro_batch(batch.select("value"))
        observations = events.select(
            F.col("key").alias("source_key"),
            F.col("value"),
            F.lit(batch_id).cast("long").alias("normalizer_batch"),
            F.monotonically_increasing_id().alias("row_in_batch"),
            F.current_timestamp().alias("written_at"),
        )
        idempotent_append(observations, paths.observations, app_id=app_id, version=batch_id)
        dead = rejected.select("key", "value", "reason", F.current_timestamp().alias("written_at"))
        idempotent_append(dead, paths.records_dlq, app_id=app_id, version=batch_id)

    return write


# -- Query 2: observation events -> state, changes ----------------------------------------------------


def read_observation_table(spark: SparkSession, uri: str, *, max_files_per_trigger: int = 0) -> DataFrame:
    """The observation table as a stream, in the columns ``stream.decode_observation_stream`` reads from Kafka."""
    reader = spark.readStream.format("delta")
    if max_files_per_trigger > 0:
        reader = reader.option("maxFilesPerTrigger", str(max_files_per_trigger))
    return reader.load(uri).select(
        F.lit(OBSERVATIONS_SOURCE).alias("source_topic"),
        F.col("normalizer_batch").alias("source_partition"),
        F.col("row_in_batch").alias("source_offset"),
        F.col("source_key"),
        F.col("value"),
        F.col("written_at").alias("kafka_timestamp"),
    )


def latest_states(states: DataFrame) -> DataFrame:
    """One row per offer: its latest STATE output in the batch."""
    from pyspark.sql import Window

    order = Window.partitionBy("offer_id").orderBy(F.col("event_time").desc(), F.col("observation_id").desc())
    return (
        states.withColumn("_rank", F.row_number().over(order))
        .filter(F.col("_rank") == 1)
        .select("offer_id", "marketplace", "observation_id", "event_time", "state_json", "updated_at")
    )


def delta_sink_writer(paths: LakehousePaths) -> Callable[[DataFrame, int], None]:
    """``foreachBatch`` function: one micro-batch of speed outputs into the Delta tables."""

    def write(batch: DataFrame, batch_id: int) -> None:
        app_id = _query_id(batch)
        stamped = batch.withColumn("batch_id", F.lit(batch_id).cast("long")).withColumn(
            "updated_at", F.current_timestamp()
        )
        stamped.persist()
        try:
            idempotent_append(stamped, paths.outputs, app_id=app_id, version=batch_id)
            states = stamped.filter(F.col("output_kind") == "STATE")
            merge_or_create(latest_states(states), paths.offer_state, offer_state_merge, "_speed_offer_state")
            changes = stamped.filter(F.col("output_kind") == "CHANGE").select(
                "change_id", "marketplace", "offer_id", "observation_id", "event_time", "change_json", "updated_at"
            )
            merge_or_create(
                changes,
                paths.price_changes,
                lambda uri, view: insert_once(uri, view, "change_id"),
                "_speed_price_changes",
            )
        finally:
            stamped.unpersist()

    return write


# -- Progress ----------------------------------------------------------------------------------------


class DeltaProgressRecorder:
    """``service.ProgressRecorder`` with a Delta table in place of ``audit.marketplace_stream_progress``."""

    def __init__(self, spark: SparkSession, uri: str, *, query_name: str) -> None:
        self.spark, self.uri, self.query_name = spark, uri, query_name
        self.seen: set[tuple[str, int]] = set()

    def record(self, progresses: Sequence[Any]) -> int:
        from ecomdata.processing.speed.service import PROGRESS_COLUMNS, progress_row, ran_a_batch

        now = datetime.now(UTC)
        rows = []
        for progress in progresses:
            if not ran_a_batch(progress):
                continue
            row = progress_row(progress, query_name=self.query_name, recorded_at=now)
            if (row["query_id"], row["batch_id"]) not in self.seen:
                rows.append(tuple(row[column] for column in PROGRESS_COLUMNS))
        if not rows:
            return 0
        frame = self.spark.createDataFrame(rows, PROGRESS_SCHEMA)
        merge_or_create(
            frame,
            self.uri,
            lambda uri, view: (
                f"MERGE INTO delta.`{uri}` AS t USING {view} AS s "
                "ON t.query_id = s.query_id AND t.batch_id = s.batch_id WHEN NOT MATCHED THEN INSERT *"
            ),
            "_speed_progress",
        )
        self.seen.update((row[1], row[2]) for row in rows)
        return len(rows)


# -- Assembly ----------------------------------------------------------------------------------------


def _trigger(writer: Any, *, available_now: bool, interval: str) -> Any:
    return writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=interval)


def start_normalizer(
    spark: SparkSession,
    paths: LakehousePaths,
    *,
    records_uri: str,
    query_name: str,
    interval: str,
    available_now: bool,
    auto_loader: bool,
    max_files_per_trigger: int,
) -> Any:
    reader = read_record_files(
        spark, records_uri, auto_loader=auto_loader, max_files_per_trigger=max_files_per_trigger
    ).writeStream.queryName(f"{query_name}-normalizer")
    if auto_loader:
        reader = reader.option("cloudFiles.schemaLocation", paths.checkpoint("normalizer-schema"))
    writer = reader.option("checkpointLocation", paths.checkpoint("normalizer")).foreachBatch(normalizer_to_delta(paths))
    return _trigger(writer, available_now=available_now, interval=interval).start()


def build_parser(default_root: str, default_records: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="The marketplace speed layer on a lakehouse: Bronze records -> Delta state and changes"
    )
    parser.add_argument("--root", default=default_root, help="URI the speed tables and checkpoints live under")
    parser.add_argument("--records-uri", default=default_records, help="the Bronze record files")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process every file present, then stop (a scheduled job instead of an always-on stream)",
    )
    parser.add_argument("--run-minutes", type=float, default=0, help="stop after this long; 0 runs until stopped")
    parser.add_argument("--auto-loader", action="store_true", help="read Bronze with Databricks Auto Loader")
    parser.add_argument("--max-files-per-trigger", type=int, default=0, help="0: the source's default")
    return parser


def main(argv: list[str] | None = None) -> None:
    from ecomdata.infra.lifecycle import StopSignal, install_signal_handlers
    from ecomdata.infra.object_store import object_store_for
    from ecomdata.infra.spark import SparkSessionFactory
    from ecomdata.processing.speed.service import await_query, change_config, start_query
    from ecomdata.settings import load_settings

    settings = load_settings(dotenv=True)
    if settings.storage.table_format != "delta":
        raise SystemExit("the lakehouse speed layer writes Delta tables: set DATA_LAKE_TABLE_FORMAT=delta")
    store = object_store_for(settings.storage)
    args = build_parser(
        store.uri("gold", "speed"), store.uri("bronze", settings.batch.records_dataset)
    ).parse_args(argv)
    paths = LakehousePaths(args.root)
    query_name = settings.speed.query_name
    spark = SparkSessionFactory.for_storage(
        settings.storage,
        app_name=f"{query_name}-lakehouse",
        shuffle_partitions=settings.spark.speed_shuffle_partitions,
        driver_memory=settings.spark.driver_memory,
        master=settings.spark.master,
        runtime=settings.spark.runtime,
    ).create()
    recorder = DeltaProgressRecorder(spark, paths.stream_progress, query_name=query_name)
    stop = StopSignal()
    try:
        install_signal_handlers(stop)
    except ValueError:
        pass  # not the main thread (a notebook): the deadline or the platform stops the run
    deadline = time.monotonic() + args.run_minutes * 60 if args.run_minutes > 0 else None
    common = {"interval": settings.speed.trigger, "available_now": args.available_now}

    normalizer = start_normalizer(
        spark,
        paths,
        records_uri=args.records_uri,
        query_name=query_name,
        auto_loader=args.auto_loader,
        max_files_per_trigger=args.max_files_per_trigger,
        **common,
    )
    query = None
    try:
        if args.available_now:
            # The stateful query reads what the normaliser wrote, so it starts once the normaliser is done.
            normalizer.awaitTermination()
        stateful = start_query(
            spark,
            config=change_config(settings.speed),
            read=lambda session: read_observation_table(
                session, paths.observations, max_files_per_trigger=args.max_files_per_trigger
            ),
            write_batch=delta_sink_writer(paths),
            checkpoint=paths.checkpoint("stateful"),
            trigger=settings.speed.trigger,
            available_now=args.available_now,
            query_name=query_name,
        )
        query = stateful
        if args.available_now:
            stateful.awaitTermination()
            recorder.record(stateful.recentProgress)
        else:

            def poll() -> None:
                recorder.record(stateful.recentProgress)
                if not normalizer.isActive:
                    raise RuntimeError(f"the normaliser query stopped: {normalizer.exception()}")
                if deadline is not None and time.monotonic() >= deadline:
                    stop.set()

            await_query(stateful, stop=stop, on_poll=poll)
        stopped = {"event": "speed_lakehouse_stopped", "root": paths.root, "stopped_at": datetime.now(UTC).isoformat()}
        print(json.dumps(stopped), flush=True)
    finally:
        for running in (normalizer, query):
            if running is not None and running.isActive:
                running.stop()
        if settings.spark.runtime != "databricks":
            spark.stop()  # a Databricks session belongs to the platform


if __name__ == "__main__":
    main()

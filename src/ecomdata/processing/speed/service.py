"""The long-running marketplace speed query.

Phase 8 plan section 6.3. ``marketplace_speed_layer`` holds the transform and
the micro-batch sink; this module assembles them into one checkpointed
Structured Streaming query and runs it until stopped.

Invalid records are carried in the query's output next to the state and
change rows, so the batch writer (``stream.BatchWriter``) sees them and the batch audit can
count them. Without that they would vanish inside the stream.

Run inside the Spark 4 image, where the Kafka connector is on the classpath:

    python -m ecomdata.processing.speed.service
"""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecomdata.domain.change_rules import ChangeRuleConfig
from ecomdata.domain.topics import MARKETPLACE_OBSERVATIONS, MARKETPLACE_RECORDS, MARKETPLACE_RECORDS_DLQ
from ecomdata.processing.speed import stream as layer
from ecomdata.processing.speed.normalizer import normalize_micro_batch
from ecomdata.processing.speed.stream import SPEED_OUTPUT_SCHEMA


def change_config(speed: Any) -> ChangeRuleConfig:
    """The change rules from the speed settings (``SpeedSettings``)."""
    return ChangeRuleConfig(
        speed.change_rule_version,
        speed.large_drop_absolute,
        speed.large_drop_relative,
        speed.stale_after_seconds,
    )


def speed_outputs(raw: DataFrame, config: ChangeRuleConfig) -> DataFrame:
    """State, change and duplicate/late rows, plus every invalid record."""
    valid, invalid = layer.decode_observation_stream(raw)
    changes = layer.build_change_stream(valid, config)
    shaped = invalid.select(
        *[
            F.col(field.name).cast(field.dataType)
            if field.name in invalid.columns
            else F.lit(None).cast(field.dataType).alias(field.name)
            for field in SPEED_OUTPUT_SCHEMA.fields
        ]
    )
    return changes.select(*SPEED_OUTPUT_SCHEMA.names).unionByName(shaped)


def start_query(
    spark: SparkSession,
    *,
    config: ChangeRuleConfig,
    read: Callable[[SparkSession], DataFrame],
    write_batch: Callable[[DataFrame, int], None],
    checkpoint: str,
    trigger: str = "30 seconds",
    query_name: str = "marketplace-speed-v1",
) -> Any:
    outputs = speed_outputs(read(spark), config)
    return (
        outputs.writeStream.queryName(query_name)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=trigger)
        .outputMode("append")
        .foreachBatch(write_batch)
        .start()
    )


def read_marketplace_records(spark: SparkSession, bootstrap_servers: str) -> DataFrame:
    """The source records Kafka holds, one ``value`` string per message."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", MARKETPLACE_RECORDS.name)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("value").cast("string").alias("value"))
    )


def normalizer_batch_writer(bootstrap_servers: str) -> Callable[[DataFrame, int], None]:
    """``foreachBatch`` function: normalise a micro-batch of records and publish the canonical events.

    Valid organic rows go to ``marketplace.observations.v1`` (the speed layer's own derived topic: this
    query is its only producer); every rejected row goes to the records DLQ with its reason. A micro-batch
    replayed after a failure republishes the same events, which the stateful query counts as duplicates.
    """

    def write(batch: DataFrame, batch_id: int) -> None:
        events, rejected = normalize_micro_batch(batch.select("value"))
        base = {"kafka.bootstrap.servers": bootstrap_servers}
        events.write.format("kafka").options(**base).option("topic", MARKETPLACE_OBSERVATIONS.name).save()
        dead = rejected.select(
            "key", F.to_json(F.struct(F.col("reason"), F.col("value").alias("record"))).alias("value")
        )
        dead.write.format("kafka").options(**base).option("topic", MARKETPLACE_RECORDS_DLQ.name).save()

    return write


def start_normalizer(
    spark: SparkSession, *, bootstrap_servers: str, checkpoint: str, trigger: str, query_name: str
) -> Any:
    return (
        read_marketplace_records(spark, bootstrap_servers)
        .writeStream.queryName(f"{query_name}-normalizer")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=trigger)
        .foreachBatch(normalizer_batch_writer(bootstrap_servers))
        .start()
    )


# -- Phase 9 plan section 6.1: Spark's own progress, one row per micro-batch --

_DURATIONS = {
    "trigger_execution_ms": "triggerExecution",
    "add_batch_ms": "addBatch",
    "get_batch_ms": "getBatch",
    "latest_offset_ms": "latestOffset",
    "query_planning_ms": "queryPlanning",
    "wal_commit_ms": "walCommit",
    "commit_offsets_ms": "commitOffsets",
}
PROGRESS_COLUMNS = (
    "query_name",
    "query_id",
    "batch_id",
    "run_id",
    "progress_at",
    "recorded_at",
    "num_input_rows",
    "input_rows_per_second",
    "processed_rows_per_second",
    "batch_duration_ms",
    *_DURATIONS,
    "state_rows_total",
    "state_memory_bytes",
    "offsets_behind_latest",
)


def _as_mapping(progress: Any) -> Mapping[str, Any]:
    """PySpark 4 hands out StreamingQueryProgress objects; their ``json`` is the
    progress exactly as Spark reported it. A plain dict passes through."""
    text = getattr(progress, "json", None)
    return json.loads(text) if isinstance(text, str) else progress


def _rate(value: Any) -> float | None:
    """Spark reports a rate over an empty interval as NaN or Infinity."""
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _offsets_behind(sources: Iterable[Mapping[str, Any]]) -> int | None:
    """How far the query trails its Kafka topics after this batch: the largest
    ``maxOffsetsBehindLatest`` its Kafka sources report (Spark sends the number as text).
    Spark commits no consumer group offsets, so this is the speed layer's lag."""
    behind = [int(float(s["metrics"]["maxOffsetsBehindLatest"])) for s in sources
              if (s.get("metrics") or {}).get("maxOffsetsBehindLatest") not in (None, "")]
    return max(behind) if behind else None


def progress_row(progress: Any, *, query_name: str, recorded_at: datetime) -> dict[str, Any]:
    p = _as_mapping(progress)
    durations = p.get("durationMs") or {}
    operators = p.get("stateOperators") or []
    return {
        "query_name": query_name,
        "query_id": str(p["id"]),
        "batch_id": int(p["batchId"]),
        "run_id": str(p["runId"]),
        "progress_at": datetime.fromisoformat(str(p["timestamp"]).replace("Z", "+00:00")),
        "recorded_at": recorded_at,
        "num_input_rows": p.get("numInputRows"),
        "input_rows_per_second": _rate(p.get("inputRowsPerSecond")),
        "processed_rows_per_second": _rate(p.get("processedRowsPerSecond")),
        "batch_duration_ms": p.get("batchDuration"),
        **{column: durations.get(key) for column, key in _DURATIONS.items()},
        "state_rows_total": sum(o.get("numRowsTotal") or 0 for o in operators) if operators else None,
        "state_memory_bytes": sum(o.get("memoryUsedBytes") or 0 for o in operators) if operators else None,
        "offsets_behind_latest": _offsets_behind(p.get("sources") or []),
    }


def ran_a_batch(progress: Any) -> bool:
    """Whether this progress reports a micro-batch that ran.

    While no data arrives, Spark reports an idle progress every ten seconds,
    under the batch id it will run *next* and without ``addBatch``. Stored,
    it would take that id's place and the real batch would be refused.
    """
    return "addBatch" in (_as_mapping(progress).get("durationMs") or {})


class ProgressRecorder:
    """Stores each micro-batch's progress once.

    ``recentProgress`` keeps the last hundred or so updates, so every poll
    sees batches already stored; those are skipped here and, after a restart,
    by the primary key. The query id is the one Spark keeps in the
    checkpoint, which is what ``audit.marketplace_speed_batch`` is keyed by.
    """

    def __init__(
        self,
        connection_factory: Callable[[], Any],
        *,
        query_name: str = "marketplace-speed-v1",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.connection_factory, self.query_name, self.clock = connection_factory, query_name, clock
        self.seen: set[tuple[str, int]] = set()

    def record(self, progresses: Iterable[Any]) -> int:
        now = self.clock()
        rows = []
        for progress in progresses:
            if not ran_a_batch(progress):
                continue
            row = progress_row(progress, query_name=self.query_name, recorded_at=now)
            key = (row["query_id"], row["batch_id"])
            if key not in self.seen:
                rows.append(row)
        if not rows:
            return 0
        placeholders = ",".join(["%s"] * len(PROGRESS_COLUMNS))
        with self.connection_factory() as conn, conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    f"INSERT INTO audit.marketplace_stream_progress ({','.join(PROGRESS_COLUMNS)}) "
                    f"VALUES ({placeholders}) ON CONFLICT (query_name, query_id, batch_id) DO NOTHING",
                    tuple(row[c] for c in PROGRESS_COLUMNS),
                )
        self.seen.update((row["query_id"], row["batch_id"]) for row in rows)
        return len(rows)


def record_progress_safely(recorder: ProgressRecorder, query: Any) -> None:
    """A measurement must never stop the stream it measures: a failure is
    logged, and the next poll retries the same batches."""
    try:
        recorder.record(query.recentProgress)
    except Exception as error:  # noqa: BLE001
        print(
            json.dumps({"event": "progress_record_failed", "error": f"{type(error).__name__}: {error}"[:500]}),
            flush=True,
        )


def await_query(query: Any, *, stop: Any, poll_seconds: float = 5, on_poll: Callable[[], None] | None = None) -> None:
    """Block until the query ends by itself, or stop it once asked to.

    ``query.stop()`` cancels a micro-batch in progress rather than waiting
    for it (seen on Spark 4.0.1). That is safe: the batch's offsets were not
    committed to the checkpoint, so the next start runs it again, and every
    sink is idempotent by deterministic ID.
    """
    while not query.awaitTermination(poll_seconds):
        if on_poll is not None:
            on_poll()
        if stop.is_set():
            query.stop()
            return


def spark_factory(settings: Any):
    from ecomdata.infra.spark import SparkSessionFactory

    return SparkSessionFactory.for_storage(
        settings.storage,
        app_name=settings.speed.query_name,
        # Fixed in the checkpoint on its first run: changing it needs a new
        # checkpoint version (and a replay).
        shuffle_partitions=settings.spark.speed_shuffle_partitions,
        driver_memory=settings.spark.driver_memory,
        master=settings.spark.master,
        # Empty inside the Spark 4 image, whose jars are baked in.
        packages=settings.kafka.spark_kafka_package or None,
    )


def sink_wiring(settings: Any) -> layer.SinkWiring:
    from ecomdata.infra.postgres import connection_factory

    def clients():
        from elasticsearch import Elasticsearch
        from redis import Redis

        from ecomdata.infra.kafka import create_producer

        return (
            create_producer(settings.kafka.bootstrap_servers),
            Elasticsearch(settings.es.host),
            Redis(host=settings.redis.host, port=settings.redis.port, db=settings.redis.db),
        )

    return layer.SinkWiring(
        clients=clients,
        connection_factory=connection_factory(settings.postgres),
        sink_options={
            "query_name": settings.speed.query_name,
            "index_changes": settings.es.index_changes,
            "index_offers": settings.es.index_offers,
            "offer_ttl_seconds": settings.redis.offer_ttl_seconds,
            "recent_changes_max": settings.redis.recent_changes_max,
            "change_ack_timeout_seconds": settings.kafka.change_ack_timeout_seconds,
        },
    )


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(
        description="Long-running marketplace speed query (Kafka -> changes, ES, Redis)"
    ).parse_args(argv)
    from ecomdata.infra.lifecycle import StopSignal, install_signal_handlers
    from ecomdata.infra.postgres import connection_factory
    from ecomdata.settings import load_settings

    settings = load_settings(dotenv=True)
    stop = StopSignal()
    install_signal_handlers(stop)
    spark = spark_factory(settings).create()
    recorder = ProgressRecorder(connection_factory(settings.postgres), query_name=settings.speed.query_name)
    normalizer = None
    writer = layer.make_batch_writer(sink_wiring(settings))
    try:
        normalizer = start_normalizer(
            spark,
            bootstrap_servers=settings.kafka.bootstrap_servers,
            checkpoint=settings.speed.checkpoint_path() + "-normalizer",
            trigger=settings.speed.trigger,
            query_name=settings.speed.query_name,
        )
        query = start_query(
            spark,
            config=change_config(settings.speed),
            read=lambda session: layer.read_marketplace_observations(
                session,
                settings.kafka.bootstrap_servers,
                max_offsets_per_trigger=settings.speed.max_offsets_per_trigger,
            ),
            write_batch=writer,
            checkpoint=settings.speed.checkpoint_path(),
            trigger=settings.speed.trigger,
            query_name=settings.speed.query_name,
        )

        def poll() -> None:
            record_progress_safely(recorder, query)
            # A normaliser that died would leave the stateful query idle without any error of its own.
            if not normalizer.isActive:
                raise RuntimeError(f"the normaliser query stopped: {normalizer.exception()}")

        await_query(query, stop=stop, on_poll=poll)
    finally:
        if normalizer is not None and normalizer.isActive:
            normalizer.stop()
        writer.close()
        spark.stop()


if __name__ == "__main__":
    main()

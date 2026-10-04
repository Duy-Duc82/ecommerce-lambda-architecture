"""The long-running marketplace speed query.

Phase 8 plan section 6.3. ``marketplace_speed_layer`` holds the transform and
the micro-batch sink; this module assembles them into one checkpointed
Structured Streaming query and runs it until stopped.

Invalid records are carried in the query's output next to the state and
change rows, so ``write_marketplace_batch`` sees them and the batch audit can
count them. Without that they would vanish inside the stream.

Run inside the Spark 4 image, where the Kafka connector is on the classpath:

    python -m speed_layer.marketplace_speed_service
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import (
    MARKETPLACE_CHANGE_RULE_VERSION, MARKETPLACE_LARGE_DROP_ABSOLUTE, MARKETPLACE_LARGE_DROP_RELATIVE,
    MARKETPLACE_SPEED_QUERY_NAME, MARKETPLACE_SPEED_SHUFFLE_PARTITIONS, MARKETPLACE_STALE_AFTER_SECONDS,
    MARKETPLACE_STREAM_TRIGGER,
    SPARK_KAFKA_PACKAGE,
)
from speed_layer import marketplace_speed_layer as layer
from speed_layer.marketplace_change_rules import ChangeRuleConfig
from speed_layer.marketplace_speed_layer import SPEED_OUTPUT_SCHEMA


def default_config() -> ChangeRuleConfig:
    return ChangeRuleConfig(MARKETPLACE_CHANGE_RULE_VERSION, MARKETPLACE_LARGE_DROP_ABSOLUTE,
                            MARKETPLACE_LARGE_DROP_RELATIVE, MARKETPLACE_STALE_AFTER_SECONDS)


def speed_outputs(raw: DataFrame, config: ChangeRuleConfig) -> DataFrame:
    """State, change and duplicate/late rows, plus every invalid record."""
    valid, invalid = layer.decode_observation_stream(raw)
    changes = layer.build_change_stream(valid, config)
    shaped = invalid.select(*[
        F.col(field.name).cast(field.dataType) if field.name in invalid.columns
        else F.lit(None).cast(field.dataType).alias(field.name)
        for field in SPEED_OUTPUT_SCHEMA.fields
    ])
    return changes.select(*SPEED_OUTPUT_SCHEMA.names).unionByName(shaped)


def start_query(
    spark: SparkSession,
    *,
    config: ChangeRuleConfig | None = None,
    read: Callable[[SparkSession], DataFrame] = layer.read_marketplace_observations,
    trigger: str = MARKETPLACE_STREAM_TRIGGER,
    query_name: str = MARKETPLACE_SPEED_QUERY_NAME,
) -> Any:
    outputs = speed_outputs(read(spark), config or default_config())
    return (outputs.writeStream
            .queryName(query_name)
            .option("checkpointLocation", layer.checkpoint_path())
            .trigger(processingTime=trigger)
            .outputMode("append")
            .foreachBatch(layer.write_marketplace_batch)
            .start())


# -- Phase 9 plan section 6.1: Spark's own progress, one row per micro-batch --

_DURATIONS = {
    "trigger_execution_ms": "triggerExecution", "add_batch_ms": "addBatch", "get_batch_ms": "getBatch",
    "latest_offset_ms": "latestOffset", "query_planning_ms": "queryPlanning", "wal_commit_ms": "walCommit",
    "commit_offsets_ms": "commitOffsets",
}
PROGRESS_COLUMNS = ("query_name", "query_id", "batch_id", "run_id", "progress_at", "recorded_at", "num_input_rows",
                    "input_rows_per_second", "processed_rows_per_second", "batch_duration_ms",
                    *_DURATIONS, "state_rows_total", "state_memory_bytes")


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

    def __init__(self, connection_factory: Callable[[], Any], *, query_name: str = MARKETPLACE_SPEED_QUERY_NAME,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
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
                cur.execute(f"INSERT INTO audit.marketplace_stream_progress ({','.join(PROGRESS_COLUMNS)}) "
                            f"VALUES ({placeholders}) ON CONFLICT (query_name, query_id, batch_id) DO NOTHING",
                            tuple(row[c] for c in PROGRESS_COLUMNS))
        self.seen.update((row["query_id"], row["batch_id"]) for row in rows)
        return len(rows)


def record_progress_safely(recorder: ProgressRecorder, query: Any) -> None:
    """A measurement must never stop the stream it measures: a failure is
    logged, and the next poll retries the same batches."""
    try:
        recorder.record(query.recentProgress)
    except Exception as error:  # noqa: BLE001
        print(json.dumps({"event": "progress_record_failed", "error": f"{type(error).__name__}: {error}"[:500]}),
              flush=True)


def await_query(query: Any, *, stop: Any, poll_seconds: float = 5,
                on_poll: Callable[[], None] | None = None) -> None:
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


def build_spark() -> SparkSession:
    builder = (SparkSession.builder.appName(MARKETPLACE_SPEED_QUERY_NAME)
               .config("spark.sql.session.timeZone", "UTC")
               # Fixed in the checkpoint on first run; see the setting.
               .config("spark.sql.shuffle.partitions", str(MARKETPLACE_SPEED_SHUFFLE_PARTITIONS)))
    # Empty inside the Spark 4 image, whose jars are baked in; set on a host
    # that must resolve the connector at start.
    if SPARK_KAFKA_PACKAGE:
        builder = builder.config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
    return builder.getOrCreate()


def main() -> None:
    argparse.ArgumentParser(description="Long-running marketplace speed query (Kafka -> changes, ES, Redis)").parse_args()
    from common.lifecycle import StopSignal, install_signal_handlers

    stop = StopSignal()
    install_signal_handlers(stop)
    from common.postgres import postgres_connection_factory

    spark = build_spark()
    recorder = ProgressRecorder(postgres_connection_factory())
    try:
        query = start_query(spark)
        await_query(query, stop=stop, on_poll=lambda: record_progress_safely(recorder, query))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

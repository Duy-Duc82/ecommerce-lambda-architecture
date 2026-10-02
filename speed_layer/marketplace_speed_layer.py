"""Spark Structured Streaming driver for marketplace observations.

The comparison itself is delegated to the pure rules module so a replay and a
batch test use exactly the same ordering and identities.
"""
from __future__ import annotations
import json
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from config.marketplace_wire import marketplace_observation_from_wire
from config.settings import (
    CHECKPOINTS_DIR, KAFKA_BOOTSTRAP_SERVERS, MARKETPLACE_CHANGE_RULE_VERSION, MARKETPLACE_SPEED_CHECKPOINT_ROOT,
    MARKETPLACE_LARGE_DROP_ABSOLUTE, MARKETPLACE_LARGE_DROP_RELATIVE,
    MARKETPLACE_STALE_AFTER_SECONDS, MARKETPLACE_STREAM_CHECKPOINT_VERSION,
    MARKETPLACE_STREAM_TRIGGER, MARKETPLACE_STREAM_WATERMARK,
)
from config.topics import MARKETPLACE_OBSERVATIONS
from speed_layer.marketplace_change_rules import (
    ChangeRuleConfig, ObservationDisposition, detect_observation_changes,
    detect_stale_change, offer_state_to_json,
)
from speed_layer.marketplace_sinks import SpeedOutput


class SpeedOutputKind(str, Enum):
    STATE = "STATE"
    CHANGE = "CHANGE"
    DUPLICATE = "DUPLICATE"
    LATE = "LATE"
    INVALID = "INVALID"


SPEED_OUTPUT_SCHEMA = StructType([
    StructField("output_kind", StringType(), False), StructField("marketplace", StringType(), True),
    StructField("offer_id", StringType(), True), StructField("observation_id", StringType(), True),
    StructField("change_id", StringType(), True), StructField("event_time", TimestampType(), True),
    StructField("state_json", StringType(), True), StructField("change_json", StringType(), True),
    StructField("source_topic", StringType(), True), StructField("source_partition", LongType(), True),
    StructField("source_offset", LongType(), True), StructField("error_type", StringType(), True),
    StructField("error_message", StringType(), True),
])


def speed_output_schema() -> StructType:
    return SPEED_OUTPUT_SCHEMA


def checkpoint_path() -> str:
    # The version is always the last component, so bumping it starts a fresh
    # checkpoint wherever the root was moved to.
    from pathlib import Path

    root = Path(MARKETPLACE_SPEED_CHECKPOINT_ROOT) if MARKETPLACE_SPEED_CHECKPOINT_ROOT else CHECKPOINTS_DIR / "marketplace_speed"
    return str((root / MARKETPLACE_STREAM_CHECKPOINT_VERSION).resolve())


def read_marketplace_observations(spark: SparkSession) -> DataFrame:
    return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", MARKETPLACE_OBSERVATIONS.name).option("startingOffsets", "earliest").load()
            .select(F.col("topic").alias("source_topic"), F.col("partition").cast("long").alias("source_partition"), F.col("offset").cast("long").alias("source_offset"), F.col("key").cast("string").alias("source_key"), F.col("value").cast("string").alias("value"), F.col("timestamp").alias("kafka_timestamp")))


def decode_observation_stream(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    from data_ingestion.schemas import MARKETPLACE_OBSERVATION_WIRE_SCHEMA
    parsed = raw.withColumn("decoded", F.from_json("value", MARKETPLACE_OBSERVATION_WIRE_SCHEMA))
    valid_condition = (
        F.col("decoded").isNotNull() & (F.col("decoded.schema_version") == "marketplace-observation.v1") &
        (F.col("decoded.event_type") == "OFFER_OBSERVED") & F.col("decoded.event_id").isNotNull() &
        F.col("decoded.payload.offer.offer_id").isNotNull() & F.col("decoded.payload.observation.observation_id").isNotNull() &
        (F.col("decoded.event_id") == F.col("decoded.payload.observation.observation_id")) &
        (F.col("decoded.payload.offer.offer_id") == F.col("decoded.payload.observation.offer_id")) &
        (F.col("decoded.occurred_at") == F.col("decoded.payload.observation.observed_at")) &
        (F.col("decoded.crawl_run_id") == F.col("decoded.payload.observation.crawl_run_id")) &
        (F.col("decoded.raw_uri") == F.col("decoded.payload.observation.raw_uri")) &
        F.col("decoded.payload.observation.raw_sha256").isNotNull() &
        F.col("decoded.payload.observation.adapter_version").isNotNull() &
        (F.col("source_key") == F.col("decoded.partition_key")) &
        (F.col("decoded.partition_key") == F.concat(F.lower("decoded.marketplace"), F.lit(":"), F.col("decoded.payload.offer.platform_listing_id"))) &
        (F.col("decoded.payload.observation.current_price").cast("decimal(38,6)") >= 0)
    )
    # ignoreNullFields=false: to_json drops null fields by default, and the
    # strict wire contract that re-reads event_json requires every key.
    valid = (parsed.filter(valid_condition)
             .select("source_topic", "source_partition", "source_offset", "kafka_timestamp", F.to_json("decoded", {"ignoreNullFields": "false"}).alias("event_json"), F.col("decoded.marketplace").alias("marketplace"), F.col("decoded.payload.offer.offer_id").alias("offer_id"), F.col("decoded.payload.observation.observation_id").alias("observation_id"), F.to_timestamp("decoded.payload.observation.observed_at").alias("event_time")))
    invalid = (parsed.filter(~valid_condition)
               .select("source_topic", "source_partition", "source_offset", F.lit("INVALID").alias("output_kind"), F.lit("OBSERVATION_CONTRACT").alias("error_type"), F.lit("invalid marketplace observation envelope, key, or lineage").alias("error_message")))
    return valid, invalid


def _process_group(rows: Iterable[dict], previous=None, config: ChangeRuleConfig | None = None) -> list[SpeedOutput]:
    config = config or ChangeRuleConfig(MARKETPLACE_CHANGE_RULE_VERSION, MARKETPLACE_LARGE_DROP_ABSOLUTE, MARKETPLACE_LARGE_DROP_RELATIVE, MARKETPLACE_STALE_AFTER_SECONDS)
    state = previous
    result: list[SpeedOutput] = []
    for row in sorted(rows, key=lambda item: (item["event_time"], item["observation_id"])):
        event = marketplace_observation_from_wire(json.loads(row["event_json"]))
        detection = detect_observation_changes(state, event, config)
        if detection.disposition is ObservationDisposition.APPLIED:
            state = detection.next_state
            result.append(SpeedOutput("STATE", event.marketplace, state.offer_id, state.observation_id, event_time=state.observed_at, state_json=offer_state_to_json(state), source_topic=row.get("source_topic"), source_partition=row.get("source_partition"), source_offset=row.get("source_offset")))
            for change in detection.changes:
                from config.marketplace_wire import canonical_json
                result.append(SpeedOutput("CHANGE", event.marketplace, state.offer_id, state.observation_id, change.event_id, change.detected_at, change_json=canonical_json(change), source_topic=row.get("source_topic"), source_partition=row.get("source_partition"), source_offset=row.get("source_offset")))
        else:
            result.append(SpeedOutput(detection.disposition.value, event.marketplace, event.payload.offer.offer_id, event.payload.observation.observation_id, event_time=event.payload.observation.observed_at, source_topic=row.get("source_topic"), source_partition=row.get("source_partition"), source_offset=row.get("source_offset")))
    return result


def make_state_function(config: ChangeRuleConfig):
    """The per-offer function ``applyInPandasWithState`` calls.

    Spark passes an *iterator* of pandas frames for one key and expects an
    iterator of frames back. All of a key's rows in a trigger are read before
    any is processed, because ``_process_group`` orders them by event time.

    With ``ProcessingTimeTimeout`` a key's timeout must be set on every call,
    or Spark drops it. It is re-armed whenever the key holds a state, from
    the latest observed instant, so an offer whose last call brought only a
    duplicate or a late row can still go stale.
    """
    import pandas as pd
    from config.marketplace_wire import canonical_json
    from speed_layer.marketplace_change_rules import offer_state_from_json

    def arm_timeout(state, observed_at) -> None:
        due_ms = int((observed_at.timestamp() + config.stale_after_seconds) * 1000)
        state.setTimeoutDuration(max(1, due_ms - state.getCurrentProcessingTimeMs()))

    def state_fn(key, frames, state):
        prior = offer_state_from_json(state.get[0]) if state.exists else None
        if state.hasTimedOut and prior is not None:
            stale_state, stale_change = detect_stale_change(prior, config)
            state.update((offer_state_to_json(stale_state), stale_state.observed_at, stale_state.observation_id))
            if stale_change is not None:
                yield pd.DataFrame([SpeedOutput("CHANGE", stale_state.marketplace, stale_state.offer_id, stale_state.observation_id,
                                                stale_change.event_id, stale_change.detected_at,
                                                change_json=canonical_json(stale_change)).__dict__])
            return
        rows = [row for frame in frames for row in frame.to_dict("records")]
        outputs = _process_group(rows, prior, config)
        latest = next((o for o in reversed(outputs) if o.output_kind == "STATE"), None)
        current = offer_state_from_json(latest.state_json) if latest else prior
        if latest:
            state.update((latest.state_json, current.observed_at, current.observation_id))
        if current is not None:
            arm_timeout(state, current.observed_at)
        if outputs:
            yield pd.DataFrame([o.__dict__ for o in outputs])

    return state_fn


def build_change_stream(valid: DataFrame, config: ChangeRuleConfig) -> DataFrame:
    """Build the deterministic grouped transform.

    On a streaming DataFrame Spark 3.5 uses the stateful pandas API; local
    batch DataFrames use the same grouping function without requiring a live
    Kafka source, which keeps the transform testable offline.
    """
    if "event_json" not in valid.columns: raise ValueError("valid stream must contain event_json")
    if valid.isStreaming and hasattr(valid.groupBy("offer_id"), "applyInPandasWithState"):
        from pyspark.sql.types import StructType
        state_schema = StructType([StructField("state_json", StringType(), False), StructField("observed_at", TimestampType(), False), StructField("observation_id", StringType(), False)])
        state_fn = make_state_function(config)
        return valid.groupBy("offer_id").applyInPandasWithState(state_fn, outputStructType=SPEED_OUTPUT_SCHEMA, stateStructType=state_schema, outputMode="Append", timeoutConf="ProcessingTimeTimeout")
    # A finite DataFrame path deliberately avoids Arrow/pandas so unit tests
    # remain runnable on a plain local Spark installation. The streaming path
    # above is the production stateful implementation.
    grouped = {}
    for row in valid.collect():
        grouped.setdefault(row.offer_id, []).append(row.asDict(recursive=True))
    outputs = []
    for rows in grouped.values():
        outputs.extend(o.__dict__ for o in _process_group(rows, config=config))
    return valid.sparkSession.createDataFrame(outputs, schema=SPEED_OUTPUT_SCHEMA)


def _sink_clients():
    from config.settings import ES_HOST, REDIS_DB, REDIS_HOST, REDIS_PORT
    from data_ingestion.marketplace_change_producer import create_change_producer
    from elasticsearch import Elasticsearch
    from redis import Redis
    return create_change_producer(), Elasticsearch(ES_HOST), Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def write_marketplace_batch(batch_df: DataFrame, batch_id: int) -> None:
    from common import postgres
    from speed_layer.marketplace_sinks import MarketplaceSpeedAudit, MarketplaceSpeedSinks
    # Spark sets the query id, which lives in the checkpoint, as a local
    # property of every micro-batch. Batch IDs are unique only within it.
    query_id = batch_df.sparkSession.sparkContext.getLocalProperty("sql.streaming.queryId")
    if not query_id: raise ValueError("micro-batch carries no streaming query id; refusing to audit it as another run's batch")
    producer, es, redis = _sink_clients()
    # A closing factory: the query runs for days, one micro-batch every
    # trigger, and a bare psycopg2 connection would leak on every audit call.
    audit = MarketplaceSpeedAudit(postgres.postgres_connection_factory())
    outputs = [SpeedOutput(**row.asDict()) for row in batch_df.collect()]
    MarketplaceSpeedSinks(producer=producer, es=es, redis=redis, audit=audit).write_batch(outputs, batch_id, query_id=query_id)

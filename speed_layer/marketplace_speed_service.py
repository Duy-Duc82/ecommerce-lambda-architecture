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
from typing import Any, Callable

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


def await_query(query: Any, *, stop: Any, poll_seconds: float = 5) -> None:
    """Block until the query ends by itself, or stop it once asked to.

    ``query.stop()`` cancels a micro-batch in progress rather than waiting
    for it (seen on Spark 4.0.1). That is safe: the batch's offsets were not
    committed to the checkpoint, so the next start runs it again, and every
    sink is idempotent by deterministic ID.
    """
    while not query.awaitTermination(poll_seconds):
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
    spark = build_spark()
    try:
        await_query(start_query(spark), stop=stop)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

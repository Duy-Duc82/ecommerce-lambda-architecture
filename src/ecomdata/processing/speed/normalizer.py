"""Speed-layer normalisation: source records in Kafka to canonical observation events.

One micro-batch of ``marketplace.records.v1`` goes through the same per-source
normalisers the batch ``silver`` stage uses (``SourceRegistry.normalizer``).
Valid sponsored rows are not observations of an offer's price, so they are
dropped here, as the batch's ``organic_only`` drops them from the marts; every
row that cannot be an observation goes to the records DLQ with its reason.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecomdata.domain.sources import SOURCES, SourceRegistry
from ecomdata.processing.normalize import common
from ecomdata.processing.normalize.wire import observation_event_json


def normalize_micro_batch(
    lines: DataFrame, registry: SourceRegistry = SOURCES
) -> tuple[DataFrame, DataFrame]:
    """(events, rejected) of a batch of record lines (a ``value`` column).

    ``events`` has ``key`` and ``value`` (event JSON); ``rejected`` has
    ``key``, ``value`` (the record line) and ``reason``.
    """
    records = common.decode_records(lines)
    known = [spec.code for spec in registry]
    events: DataFrame | None = None
    rejected: list[DataFrame] = []
    for code in known:
        rows = common.listing_rows(records.filter(F.col("source_code") == code))
        valid, quarantined = common.split_quarantine(registry.normalizer(code).to_offer_observation(rows))
        organic = valid.filter(~F.coalesce(F.col("is_advertisement"), F.lit(False)))
        encoded = observation_event_json(organic)
        events = encoded if events is None else events.unionByName(encoded)
        rejected.append(
            quarantined.select(
                F.col("record_id").alias("key"), F.col("payload_json").alias("value"), F.col("reason")
            )
        )
    unsupported = common.unsupported_envelopes(records).select(
        F.col("record_id").alias("key"), F.col("payload_json").alias("value"), F.lit("unsupported_envelope").alias("reason")
    )
    unknown = common.listing_rows(
        records.filter(~F.col("source_code").isin(*known) | F.col("source_code").isNull())
    ).select(F.col("record_id").alias("key"), F.col("payload_json").alias("value"), F.lit("unknown_source").alias("reason"))
    rejected.extend([unsupported, unknown])
    all_rejected = rejected[0]
    for frame in rejected[1:]:
        all_rejected = all_rejected.unionByName(frame)
    return events, all_rejected

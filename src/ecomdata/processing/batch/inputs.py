"""The views of Silver and of the crawl audit a run reads: the as_of cut, organic rows, one row per observation."""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecomdata.domain.validation import aware_utc
from ecomdata.processing.batch.context import ConflictingObservationError

# Which Bronze record a Silver row came from; two copies of one observation differ in these only.
LINEAGE_COLUMNS = ("record_id", "raw_artifact_id")


def observations_as_of(silver: DataFrame, as_of: datetime) -> DataFrame:
    """Silver as it stood at ``as_of``: nothing observed after it, inclusive.

    A run is a view of one instant. Without this, Silver that kept growing
    after as_of leaks into a rerun or a backfill, so the same context stops
    producing the same Gold and an older window can never pass its gates.
    """
    cutoff = F.lit(aware_utc(as_of, "as_of")).cast("timestamp")
    return silver.filter(F.col("observed_at") <= cutoff)


def fetched_as_of(frame: DataFrame, as_of: datetime) -> DataFrame:
    """Rows of a Silver table keyed by fetch time (``page_context``) as they stood at ``as_of``."""
    cutoff = F.lit(aware_utc(as_of, "as_of")).cast("timestamp")
    return frame.filter(F.col("fetched_at") <= cutoff)


def organic_only(silver: DataFrame) -> DataFrame:
    """Without sponsored placements.

    A sponsored row rotates between loads and ranks nowhere, so it would distort
    price history and rank. It stays in Silver, marked, for analysis; the marts
    and the gates judge the organic listing. (The pre-ELT adapters dropped these
    rows before Silver ever saw them.)
    """
    return silver.filter(~F.coalesce(F.col("is_advertisement"), F.lit(False)))


def crawl_audit_as_of(attempts: DataFrame, runs: DataFrame, as_of: datetime) -> tuple[DataFrame, DataFrame]:
    """The crawl audit as it stood at ``as_of``.

    An attempt is known once it completed, so one still in flight at as_of
    reported nothing yet. A crawl run exists once it started, finished or not;
    whether it had settled is check 8's question, not this cut's.
    """
    cutoff = F.lit(aware_utc(as_of, "as_of")).cast("timestamp")
    return attempts.filter(F.col("completed_at") <= cutoff), runs.filter(F.col("started_at") <= cutoff)


def deduplicate_observations(observations: DataFrame) -> DataFrame:
    """One row per ``observation_id``; the same id with different content is an error, never a choice."""
    # The lineage columns name the record a row came from, not what it says.
    semantic_cols = [name for name in observations.columns if name not in LINEAGE_COLUMNS]
    marked = observations.withColumn(
        "_content_hash", F.sha2(F.to_json(F.struct(*[F.col(x) for x in semantic_cols])), 256)
    )
    conflicts = (
        marked.groupBy("observation_id").agg(F.countDistinct("_content_hash").alias("hashes")).filter("hashes > 1")
    )
    conflict_ids = [row.observation_id for row in conflicts.collect()]
    if conflict_ids:
        raise ConflictingObservationError(
            f"conflicting canonical content for observation IDs: {sorted(conflict_ids)[:10]}"
        )
    return marked.drop("_content_hash").dropDuplicates(["observation_id"])


def duplicate_counts(observations: DataFrame) -> dict[str, int]:
    """Rows, and how many of them repeat an ``observation_id`` or repeat a whole row, before any dedup.

    ``observation_id`` is deterministic from (source, listing, instant, body), so a
    repeat means the same listing twice in one response (or a replayed record).
    ``identical_content`` counts the repeats whose columns, bar the lineage ones,
    equal another row's; ``observation_id`` minus ``identical_content`` is the
    number of repeats that carry different content, which dedup refuses.
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


def judged_observations(silver: DataFrame, as_of: datetime) -> DataFrame:
    """The Silver rows a run judges: as of the cut, organic only (not yet deduplicated)."""
    return organic_only(observations_as_of(silver, as_of))

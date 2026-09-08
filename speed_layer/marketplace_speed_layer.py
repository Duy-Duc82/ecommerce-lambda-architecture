"""Marketplace speed layer: observations in, bounded change events out.

Scope limit stated plainly: ``foreachBatch`` collects each micro-batch to the
driver and processes it there, so the state store and the sink clients are never
serialized to executors.  That is correct for a thesis-scale stream bounded by
``SPEED_MAX_OFFSETS_PER_TRIGGER``; it is not a design for unbounded throughput,
and this module does not pretend it is.

Sink ordering is mandatory and is the whole reason a crash here is safe:

    detect -> publish changes -> index documents -> advance state -> audit

State advance is last.  A failure anywhere before it leaves the stored previous
state untouched, so the replay recomputes the identical change set with
identical deterministic IDs and every sink write becomes an idempotent
overwrite.  This is the speed-layer analogue of the acquisition layer's
raw-first rule: never advance the pointer that makes the evidence
unreproducible.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence

from config.marketplace_schema import (
    MarketplaceChangeType,
    MarketplaceChangeV1,
    MarketplaceObservationV1,
)
from config.settings import (
    CHANGE_RULE_VERSION,
    ES_INDEX_MARKETPLACE_CHANGES,
    ES_INDEX_MARKETPLACE_OBSERVATIONS,
    SPEED_LARGE_DROP_ABSOLUTE,
    SPEED_LARGE_DROP_PERCENT,
    SPEED_STALE_SWEEP_LIMIT,
    SPEED_STALE_THRESHOLD_MINUTES,
)
from serving_layer.marketplace_es import change_document, observation_document
from speed_layer.change_rules import (
    ChangeDetectionResult,
    ChangeThresholds,
    ObservationOutcome,
    OfferStateSnapshot,
    detect_changes,
    detect_stale_offers,
    state_from_observation,
)
from speed_layer.offer_state import OfferStateStore

logger = logging.getLogger(__name__)

STAGE_DECODE = "DECODE"
STAGE_PUBLISH = "PUBLISH"
STAGE_INDEX = "INDEX"
STAGE_STATE = "STATE"
STAGE_SERVING = "SERVING"

STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"


def default_thresholds() -> ChangeThresholds:
    return ChangeThresholds(
        large_drop_absolute=SPEED_LARGE_DROP_ABSOLUTE,
        large_drop_percent=SPEED_LARGE_DROP_PERCENT,
        stale_after=timedelta(minutes=SPEED_STALE_THRESHOLD_MINUTES),
    )


@dataclass(frozen=True)
class MicroBatchReport:
    batch_id: int
    started_at: datetime
    completed_at: datetime
    observations_in: int
    detected: int
    duplicates: int
    out_of_order: int
    conflicts: int
    decode_failures: int
    changes_published: int
    changes_by_type: dict[str, int] = field(default_factory=dict)
    rule_version: str = CHANGE_RULE_VERSION
    status: str = STATUS_SUCCEEDED
    failure_stage: str | None = None
    failure_message: str | None = None

    def __post_init__(self) -> None:
        accounted = (
            self.detected
            + self.duplicates
            + self.out_of_order
            + self.conflicts
            + self.decode_failures
        )
        if accounted != self.observations_in:
            raise ValueError(
                "micro-batch counts do not reconcile: "
                f"{accounted} accounted for {self.observations_in} observations"
            )
        if self.status == STATUS_FAILED and self.failure_stage is None:
            raise ValueError("a failed report requires a failure stage")
        if self.status == STATUS_SUCCEEDED and self.failure_stage is not None:
            raise ValueError("a successful report cannot carry a failure stage")


def _decoder() -> Callable[[Mapping[str, Any]], MarketplaceObservationV1]:
    """Phase 4's strict wire decoder, imported lazily.

    Kept as a lazy lookup rather than a local copy so the speed layer picks up
    the accepted decoder when Phase 4 lands, and so a missing Phase 4 produces
    one clear error instead of a second, divergent decode implementation.
    """
    from config.marketplace_wire import marketplace_observation_from_wire  # type: ignore

    return marketplace_observation_from_wire


@dataclass
class _Counters:
    detected: int = 0
    duplicates: int = 0
    out_of_order: int = 0
    conflicts: int = 0
    decode_failures: int = 0

    def record(self, outcome: ObservationOutcome) -> None:
        if outcome is ObservationOutcome.DETECTED:
            self.detected += 1
        elif outcome is ObservationOutcome.DUPLICATE:
            self.duplicates += 1
        elif outcome is ObservationOutcome.OUT_OF_ORDER:
            self.out_of_order += 1
        else:
            self.conflicts += 1


def _group_by_offer(
    events: Sequence[MarketplaceObservationV1],
) -> list[tuple[str, list[MarketplaceObservationV1]]]:
    """Group by offer and order each group by observed time, ID as tiebreak."""
    grouped: dict[str, list[MarketplaceObservationV1]] = {}
    for event in events:
        grouped.setdefault(event.payload.offer.offer_id, []).append(event)
    for group in grouped.values():
        group.sort(
            key=lambda event: (
                event.payload.observation.observed_at,
                event.payload.observation.observation_id,
            )
        )
    return sorted(grouped.items())


def process_micro_batch(
    *,
    batch_id: int,
    records: Sequence[Mapping[str, Any]],
    state_store: OfferStateStore,
    change_publisher: Callable[[Sequence[MarketplaceChangeV1]], None],
    es_writer: Callable[[str, Sequence[tuple[str, Mapping[str, Any]]]], None],
    redis_writer,
    thresholds: ChangeThresholds,
    rule_version: str,
    clock: Callable[[], datetime],
    decoder: Callable[[Mapping[str, Any]], MarketplaceObservationV1] | None = None,
    audit_writer: Callable[[MicroBatchReport], None] | None = None,
    observations_index: str = ES_INDEX_MARKETPLACE_OBSERVATIONS,
    changes_index: str = ES_INDEX_MARKETPLACE_CHANGES,
) -> MicroBatchReport:
    """Detect and serve the changes in one micro-batch."""
    started_at = clock()
    decode = decoder or _decoder()
    counters = _Counters()
    changes: list[MarketplaceChangeV1] = []
    change_docs: list[tuple[str, Mapping[str, Any]]] = []
    observation_docs: list[tuple[str, Mapping[str, Any]]] = []
    next_states: list[OfferStateSnapshot] = []
    changes_by_type: dict[str, int] = {}
    latest_observation_at: dict[str, datetime] = {}
    latest_change_at: dict[str, datetime] = {}

    def report(
        *,
        status: str = STATUS_SUCCEEDED,
        stage: str | None = None,
        message: str | None = None,
        published: int = 0,
    ) -> MicroBatchReport:
        return MicroBatchReport(
            batch_id=batch_id,
            started_at=started_at,
            completed_at=clock(),
            observations_in=len(records),
            detected=counters.detected,
            duplicates=counters.duplicates,
            out_of_order=counters.out_of_order,
            conflicts=counters.conflicts,
            decode_failures=counters.decode_failures,
            changes_published=published,
            changes_by_type=dict(changes_by_type),
            rule_version=rule_version,
            status=status,
            failure_stage=stage,
            failure_message=message,
        )

    def finish(result: MicroBatchReport) -> MicroBatchReport:
        if audit_writer is not None:
            audit_writer(result)
        return result

    # --- decode ---------------------------------------------------------
    events: list[MarketplaceObservationV1] = []
    for record in records:
        try:
            events.append(decode(record))
        except Exception as exc:
            # Phase 4's Silver sink already owns bad-record handling for this
            # topic; republishing here would put two DLQ records on one bad
            # message and double-count the failure.
            counters.decode_failures += 1
            logger.warning(
                "speed layer: undecodable record skipped in batch %s: %s", batch_id, exc
            )

    # --- detect ---------------------------------------------------------
    grouped = _group_by_offer(events)
    stored = state_store.get_many([offer_id for offer_id, _ in grouped])
    for offer_id, group in grouped:
        previous = stored.get(offer_id)
        for event in group:
            result: ChangeDetectionResult = detect_changes(
                previous=previous,
                event=event,
                detected_at=started_at,
                thresholds=thresholds,
                rule_version=rule_version,
            )
            counters.record(result.outcome)
            if result.outcome is not ObservationOutcome.DETECTED:
                continue
            current_state = result.next_state
            assert current_state is not None  # guaranteed by ChangeDetectionResult
            observation_docs.append((event.event_id, observation_document(event)))
            marketplace = event.marketplace
            observed_at = event.payload.observation.observed_at
            prior = latest_observation_at.get(marketplace)
            if prior is None or observed_at > prior:
                latest_observation_at[marketplace] = observed_at
            for change in result.changes:
                changes.append(change)
                changes_by_type[change.change_type.value] = (
                    changes_by_type.get(change.change_type.value, 0) + 1
                )
                change_docs.append(
                    (
                        change.event_id,
                        change_document(
                            change,
                            state=current_state,
                            previous_observed_at=None if previous is None else previous.observed_at,
                            counter_reset_or_invalid=result.counter_reset_or_invalid,
                            secondary_rating_field=result.secondary_rating_field,
                        ),
                    )
                )
                latest_change_at[marketplace] = change.detected_at
            # Fold forward in memory: comparing every event in the batch
            # against one starting snapshot would collapse several real
            # transitions into a single reported change.
            previous = current_state
        if previous is not None and (
            offer_id not in stored or stored[offer_id].observation_id != previous.observation_id
        ):
            next_states.append(previous)

    # --- publish, index, then advance state -----------------------------
    try:
        if changes:
            change_publisher(changes)
    except Exception as exc:
        acked = getattr(exc, "acked", 0)
        finish(report(status=STATUS_FAILED, stage=STAGE_PUBLISH, message=str(exc), published=acked))
        raise

    try:
        if observation_docs:
            es_writer(observations_index, observation_docs)
        if change_docs:
            es_writer(changes_index, change_docs)
    except Exception as exc:
        finish(
            report(
                status=STATUS_FAILED,
                stage=STAGE_INDEX,
                message=str(exc),
                published=len(changes),
            )
        )
        raise

    try:
        if next_states:
            state_store.put_many(next_states)
    except Exception as exc:
        finish(
            report(
                status=STATUS_FAILED,
                stage=STAGE_STATE,
                message=str(exc),
                published=len(changes),
            )
        )
        raise

    try:
        if change_docs:
            redis_writer.write_changes(changes, dict(change_docs))
        for marketplace, observed_at in sorted(latest_observation_at.items()):
            redis_writer.write_source_freshness(
                marketplace=marketplace,
                last_observation_at=observed_at,
                last_change_at=latest_change_at.get(marketplace),
            )
    except Exception as exc:
        finish(
            report(
                status=STATUS_FAILED,
                stage=STAGE_SERVING,
                message=str(exc),
                published=len(changes),
            )
        )
        raise

    return finish(report(published=len(changes)))


def run_stale_sweep(
    *,
    state_store: OfferStateStore,
    change_publisher: Callable[[Sequence[MarketplaceChangeV1]], None],
    es_writer: Callable[[str, Sequence[tuple[str, Mapping[str, Any]]]], None],
    redis_writer,
    thresholds: ChangeThresholds,
    rule_version: str,
    clock: Callable[[], datetime],
    limit: int = SPEED_STALE_SWEEP_LIMIT,
    batch_id: int = -1,
    audit_writer: Callable[[MicroBatchReport], None] | None = None,
    changes_index: str = ES_INDEX_MARKETPLACE_CHANGES,
) -> MicroBatchReport:
    """Emit OFFER_STALE for offers whose last observation is too old.

    Bounded by ``limit`` and idempotent: the change identity ties to the last
    observation, so repeating the sweep rewrites the same documents rather than
    emitting a new stale event every time it runs.
    """
    started_at = clock()
    states = state_store.scan_states(limit)
    by_offer = {state.offer_id: state for state in states}
    changes = detect_stale_offers(
        states=states,
        now=started_at,
        thresholds=thresholds,
        rule_version=rule_version,
        limit=limit,
    )
    changes_by_type = (
        {MarketplaceChangeType.OFFER_STALE.value: len(changes)} if changes else {}
    )
    documents = [
        (
            change.event_id,
            change_document(
                change,
                state=by_offer[change.offer_id],
                previous_observed_at=by_offer[change.offer_id].observed_at,
            ),
        )
        for change in changes
    ]

    def report(
        *, status: str = STATUS_SUCCEEDED, stage: str | None = None, message: str | None = None, published: int = 0
    ) -> MicroBatchReport:
        return MicroBatchReport(
            batch_id=batch_id,
            started_at=started_at,
            completed_at=clock(),
            observations_in=0,
            detected=0,
            duplicates=0,
            out_of_order=0,
            conflicts=0,
            decode_failures=0,
            changes_published=published,
            changes_by_type=dict(changes_by_type),
            rule_version=rule_version,
            status=status,
            failure_stage=stage,
            failure_message=message,
        )

    def finish(result: MicroBatchReport) -> MicroBatchReport:
        if audit_writer is not None:
            audit_writer(result)
        return result

    try:
        if changes:
            change_publisher(changes)
    except Exception as exc:
        finish(report(status=STATUS_FAILED, stage=STAGE_PUBLISH, message=str(exc), published=getattr(exc, "acked", 0)))
        raise
    try:
        if documents:
            es_writer(changes_index, documents)
        if documents:
            redis_writer.write_changes(changes, dict(documents))
        for marketplace in sorted({change.marketplace for change in changes}):
            redis_writer.write_source_freshness(
                marketplace=marketplace, last_stale_sweep_at=started_at
            )
    except Exception as exc:
        finish(report(status=STATUS_FAILED, stage=STAGE_SERVING, message=str(exc), published=len(changes)))
        raise
    # Staleness intentionally does not advance state: the offer is stale
    # precisely because no new observation exists to advance it to.
    return finish(report(published=len(changes)))


def postgres_audit_writer(connect: Callable[[], Any]) -> Callable[[MicroBatchReport], None]:
    """Persist micro-batch reports to audit.speed_micro_batch."""
    import json

    def write(report: MicroBatchReport) -> None:
        connection = connect()
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO audit.speed_micro_batch (
                            batch_id, rule_version, started_at, completed_at,
                            observations_in, detected, duplicates, out_of_order,
                            conflicts, decode_failures, changes_published,
                            changes_by_type, status, failure_stage, failure_message
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (rule_version, batch_id) DO UPDATE SET
                            completed_at = EXCLUDED.completed_at,
                            observations_in = EXCLUDED.observations_in,
                            detected = EXCLUDED.detected,
                            duplicates = EXCLUDED.duplicates,
                            out_of_order = EXCLUDED.out_of_order,
                            conflicts = EXCLUDED.conflicts,
                            decode_failures = EXCLUDED.decode_failures,
                            changes_published = EXCLUDED.changes_published,
                            changes_by_type = EXCLUDED.changes_by_type,
                            status = EXCLUDED.status,
                            failure_stage = EXCLUDED.failure_stage,
                            failure_message = EXCLUDED.failure_message
                        """,
                        (
                            report.batch_id,
                            report.rule_version,
                            report.started_at,
                            report.completed_at,
                            report.observations_in,
                            report.detected,
                            report.duplicates,
                            report.out_of_order,
                            report.conflicts,
                            report.decode_failures,
                            report.changes_published,
                            json.dumps(report.changes_by_type, sort_keys=True),
                            report.status,
                            report.failure_stage,
                            report.failure_message,
                        ),
                    )
        finally:
            connection.close()

    return write


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def run_speed_layer(
    *,
    bootstrap_servers: str | None = None,
    checkpoint_dir: str | None = None,
    max_offsets_per_trigger: int | None = None,
    clock: Callable[[], datetime] = _utc_now,
) -> None:
    """Spark Structured Streaming entry point.

    Every pyspark import lives inside this function so the module — and its
    unit tests — load without pyspark installed.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from config.settings import (
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS,
        REDIS_MARKETPLACE_NAMESPACE,
        SPEED_CHANGE_DOC_TTL_SECONDS,
        SPEED_CHECKPOINT_DIR,
        SPEED_FRESHNESS_THRESHOLD_MINUTES,
        SPEED_MAX_OFFSETS_PER_TRIGGER,
        SPEED_RECENT_CHANGES_MAX,
        SPARK_KAFKA_PACKAGE,
    )
    from data_ingestion.schemas import MARKETPLACE_OBSERVATION_WIRE_SCHEMA
    from data_ingestion.marketplace_change_producer import (
        create_change_producer,
        publish_changes,
    )
    from serving_layer.marketplace_es import ensure_indices, index_documents
    from serving_layer.marketplace_redis import MarketplaceRedisWriter
    from speed_layer.offer_state import RedisOfferStateStore

    import redis as redis_client_module
    from elasticsearch import Elasticsearch

    from config.settings import ES_HOST, REDIS_DB, REDIS_HOST, REDIS_PORT

    servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
    spark = (
        SparkSession.builder.appName("marketplace-speed-layer")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    redis_conn = redis_client_module.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True
    )
    es_conn = Elasticsearch(ES_HOST)
    ensure_indices(
        es_conn,
        observations_index=ES_INDEX_MARKETPLACE_OBSERVATIONS,
        changes_index=ES_INDEX_MARKETPLACE_CHANGES,
    )
    producer = create_change_producer(bootstrap_servers=servers)
    state_store = RedisOfferStateStore(redis_conn, namespace=REDIS_MARKETPLACE_NAMESPACE)
    redis_writer = MarketplaceRedisWriter(
        redis_conn,
        namespace=REDIS_MARKETPLACE_NAMESPACE,
        recent_changes_max=SPEED_RECENT_CHANGES_MAX,
        change_doc_ttl_seconds=SPEED_CHANGE_DOC_TTL_SECONDS,
        freshness_threshold_minutes=SPEED_FRESHNESS_THRESHOLD_MINUTES,
    )
    thresholds = default_thresholds()

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", KAFKA_TOPIC_MARKETPLACE_OBSERVATIONS)
        .option("startingOffsets", "earliest")
        .option(
            "maxOffsetsPerTrigger",
            max_offsets_per_trigger or SPEED_MAX_OFFSETS_PER_TRIGGER,
        )
        .load()
        .select(
            F.from_json(
                F.col("value").cast("string"), MARKETPLACE_OBSERVATION_WIRE_SCHEMA
            ).alias("event")
        )
        .select("event.*")
    )

    def handle_batch(batch_df, batch_id: int) -> None:
        records = [row.asDict(recursive=True) for row in batch_df.collect()]
        report = process_micro_batch(
            batch_id=batch_id,
            records=records,
            state_store=state_store,
            change_publisher=lambda changes: publish_changes(producer, changes),
            es_writer=lambda index, docs: index_documents(es_conn, index, docs),
            redis_writer=redis_writer,
            thresholds=thresholds,
            rule_version=CHANGE_RULE_VERSION,
            clock=clock,
        )
        # Deliberately not caught: a swallowed foreachBatch exception is the
        # classic way a streaming job reports health while losing data.
        logger.info(
            "batch %s: in=%d detected=%d changes=%d duplicates=%d out_of_order=%d conflicts=%d decode_failures=%d",
            batch_id,
            report.observations_in,
            report.detected,
            report.changes_published,
            report.duplicates,
            report.out_of_order,
            report.conflicts,
            report.decode_failures,
        )

    query = (
        stream.writeStream.foreachBatch(handle_batch)
        .option("checkpointLocation", checkpoint_dir or str(SPEED_CHECKPOINT_DIR))
        .start()
    )
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser(description="Marketplace speed layer")
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--checkpoint-dir")
    parser.add_argument("--max-offsets-per-trigger", type=int)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    run_speed_layer(
        bootstrap_servers=args.bootstrap_servers,
        checkpoint_dir=args.checkpoint_dir,
        max_offsets_per_trigger=args.max_offsets_per_trigger,
    )


if __name__ == "__main__":
    main()

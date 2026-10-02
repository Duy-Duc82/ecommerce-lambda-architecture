"""The long-running Kafka-to-Silver service.

Phase 8 plan section 6.2. ``process_record`` decides what one record becomes —
a Silver object, or a quarantined record plus a DLQ message. This module owns
the consumer loop around it and the one rule that makes the sink lossless:
**an offset is committed only after its record was processed.**

A crash between write and commit re-delivers the record. That is safe: the
Silver path is a function of ``observation_id``, so the rewrite produces the
same object. A record whose processing raises is retried at the same offset,
with capped doubling backoff, and never skipped.

Importing this module opens nothing; Kafka and MinIO are built in :func:`main`.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Callable

from data_ingestion.marketplace_silver_sink import SourceRecord


def _offset(tp: Any, offset: int) -> dict:
    try:
        from kafka.structs import OffsetAndMetadata
    except ImportError:  # the offline suite has no client; the shape is the same
        from collections import namedtuple

        OffsetAndMetadata = namedtuple("OffsetAndMetadata", ["offset", "metadata"])
    return {tp: OffsetAndMetadata(offset, None)}


def run_sink(
    consumer: Any,
    *,
    process: Callable[[SourceRecord], Any],
    stop: Any,
    poll_timeout_ms: int,
    retry_base_seconds: float,
    retry_max_seconds: float,
    max_records: int | None = None,
    log: Callable[[str], None] = print,
    commit_offsets: Callable[[Any, int], dict] = _offset,
    beat: Callable[[], None] = lambda: None,
) -> int:
    """Consume until stopped, or until ``max_records`` were handled; return that count."""
    if poll_timeout_ms <= 0:
        raise ValueError("poll_timeout_ms must be positive")
    if retry_base_seconds <= 0:
        raise ValueError("retry_base_seconds must be positive")
    if retry_max_seconds <= 0 or retry_max_seconds < retry_base_seconds:
        raise ValueError("retry_max_seconds must be positive and at least retry_base_seconds")
    handled = 0
    delay = retry_base_seconds
    while not stop.is_set():
        batch = consumer.poll(timeout_ms=poll_timeout_ms)
        # Once per poll, retrying or not: a sink waiting out a storage outage
        # is alive, and its failures show in the log and the drills.
        beat()
        # poll() has already advanced every partition past what it returned.
        # Whatever is left unprocessed in this batch is rewound before it is
        # abandoned, or it would be skipped for good.
        pending = {tp: list(records) for tp, records in batch.items() if records}
        failed = False
        for tp in list(pending):
            while pending[tp]:
                if stop.is_set() or (max_records is not None and handled >= max_records):
                    break
                message = pending[tp][0]
                record = SourceRecord(message.topic, message.partition, message.offset, message.key, message.value)
                try:
                    outcome = process(record)
                except Exception as error:
                    log(json.dumps({"event": "silver_record_failed", "partition": record.partition,
                                    "offset": record.offset, "error": f"{type(error).__name__}: {error}"[:500],
                                    "retry_in_seconds": delay}, sort_keys=True))
                    failed = True
                    break
                consumer.commit(offsets=commit_offsets(tp, record.offset + 1))
                pending[tp].pop(0)
                handled += 1
                delay = retry_base_seconds
                log(json.dumps({"event": "silver_record", "partition": record.partition, "offset": record.offset,
                                "status": getattr(outcome, "status", None)}, sort_keys=True))
            if failed:
                break
        for tp, records in pending.items():
            if records:
                consumer.seek(tp, records[0].offset)
        if max_records is not None and handled >= max_records:
            break
        if failed:
            if stop.wait(delay):
                break
            delay = min(delay * 2, retry_max_seconds)
    return handled


def create_consumer(bootstrap_servers: str | None = None) -> Any:
    from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_SILVER_CONSUMER_GROUP
    from config.topics import MARKETPLACE_OBSERVATIONS

    try:
        from kafka import KafkaConsumer
    except ImportError as exc:  # pragma: no cover - environment, not logic
        raise ImportError("kafka-python-ng is required") from exc
    # Manual commits only: an automatic commit could acknowledge a record
    # whose Silver write had not happened yet.
    return KafkaConsumer(
        MARKETPLACE_OBSERVATIONS.name,
        bootstrap_servers=bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_SILVER_CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Long-running Kafka-to-Silver sink for marketplace observations")
    parser.add_argument("--max-records", type=int, default=None, help="stop after N records (tests and smoke only)")
    args = parser.parse_args()

    from common.heartbeat import heartbeat
    from common.lifecycle import StopSignal, install_signal_handlers
    from common.object_store import put_bytes
    from config.settings import (
        MARKETPLACE_SILVER_POLL_TIMEOUT_MS, MARKETPLACE_SILVER_RETRY_BASE_SECONDS, MARKETPLACE_SILVER_RETRY_MAX_SECONDS,
        SERVICE_HEARTBEAT_FILE,
    )
    from data_ingestion.marketplace_producer import create_marketplace_producer
    from data_ingestion.marketplace_silver_sink import process_record

    stop = StopSignal()
    install_signal_handlers(stop)
    consumer = create_consumer()
    dlq_producer = create_marketplace_producer()
    clock = lambda: datetime.now(timezone.utc)  # noqa: E731 - the DLQ failed_at clock
    try:
        run_sink(
            consumer,
            process=lambda record: process_record(record, writer=put_bytes, dlq_producer=dlq_producer, clock=clock),
            stop=stop,
            poll_timeout_ms=MARKETPLACE_SILVER_POLL_TIMEOUT_MS,
            retry_base_seconds=MARKETPLACE_SILVER_RETRY_BASE_SECONDS,
            retry_max_seconds=MARKETPLACE_SILVER_RETRY_MAX_SECONDS,
            max_records=args.max_records,
            log=lambda line: print(line, flush=True),
            beat=heartbeat(SERVICE_HEARTBEAT_FILE),
        )
    finally:
        consumer.close()
        dlq_producer.flush()
        dlq_producer.close()


if __name__ == "__main__":
    main()

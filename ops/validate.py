"""Read-only checks over the running stack — Phase 8 plan section 9.2.

Each check returns one row (``check``, ``status``, ``observed``, ``expected``),
and the report is sorted by check name. ``validate`` exits non-zero if any
check fails; a check that raises is a failure, with the error as its
observation.

Every verdict is derived from the data, never from a wall clock. Windows are
anchored on instants the stack recorded itself: the newest crawl attempt, the
serving pointer's ``as_of``, the newest offer state.

The checks reach the stack only through :class:`Sources`. The default suite
drives them with a fake; :class:`LiveSources` is the real one, built in the
``ops`` container.
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Protocol, Sequence
from urllib.parse import unquote, urlparse

PASS, FAIL = "PASS", "FAIL"
SCHEDULED_RUN_PREFIX = "mp-"


@dataclass(frozen=True)
class CheckResult:
    check: str
    status: str
    observed: Any
    expected: Any


class Sources(Protocol):
    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]: ...
    def object_exists(self, uri: str) -> bool: ...
    def list_objects(self, uri_prefix: str) -> list[str]: ...
    def read_object(self, uri: str) -> bytes | None: ...
    def parquet_rows(self, uri_prefix: str) -> int | None: ...
    def consumer_lag(self, group: str, topic: str) -> int: ...
    def dlq_stages(self) -> list[str]: ...
    def es_documents(self, index: str) -> list[dict]: ...
    def redis_exists(self, key: str) -> bool: ...
    def current_pointer(self) -> dict | None: ...


@dataclass(frozen=True)
class Limits:
    reconciliation_lookback_seconds: int
    reconciliation_settle_seconds: int
    offer_ttl_seconds: int
    max_silver_lag: int = 0


def _result(name: str, ok: bool, observed: Any, expected: Any) -> CheckResult:
    return CheckResult(name, PASS if ok else FAIL, observed, expected)


def _instant(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def bronze_present(src: Sources, limits: Limits) -> CheckResult:
    rows = src.query(
        "SELECT raw_uri FROM audit.crawl_request_attempt WHERE raw_uri IS NOT NULL AND started_at >= "
        "(SELECT max(started_at) FROM audit.crawl_request_attempt) - make_interval(secs => %s)",
        (limits.reconciliation_lookback_seconds,),
    )
    missing = sorted(uri for (uri,) in rows if not src.object_exists(uri))
    return _result("bronze_present", not missing, {"attempts": len(rows), "missing": missing[:10]}, {"missing": []})


def kafka_to_silver_lag(src: Sources, limits: Limits) -> CheckResult:
    from config.settings import KAFKA_SILVER_CONSUMER_GROUP
    from config.topics import MARKETPLACE_OBSERVATIONS

    lag = src.consumer_lag(KAFKA_SILVER_CONSUMER_GROUP, MARKETPLACE_OBSERVATIONS.name)
    return _result("kafka_to_silver_lag", lag <= limits.max_silver_lag, lag, f"<= {limits.max_silver_lag}")


def _silver_rows_by_run(src: Sources, *, as_of: datetime, oldest: datetime) -> dict[str, list[datetime]]:
    """Observed instants per crawl run, for Silver rows observed at or before ``as_of``."""
    from config.settings import MARKETPLACE_SILVER_DATASET, data_lake_uri

    root = data_lake_uri("silver", MARKETPLACE_SILVER_DATASET)
    runs: dict[str, list[datetime]] = {}
    for uri in src.list_objects(root):
        # The path carries observed_date: skip days wholly outside the window.
        day = next((part.split("=", 1)[1] for part in uri.split("/") if part.startswith("observed_date=")), None)
        if day is not None and not (oldest.date() <= datetime.fromisoformat(day).date() <= as_of.date()):
            continue
        payload = src.read_object(uri)
        if payload is None:
            continue
        event = json.loads(payload)
        observed = _instant(event["payload"]["observation"]["observed_at"])
        if observed <= as_of:
            runs.setdefault(event["crawl_run_id"], []).append(observed)
    return runs


def silver_reconciles_with_audit(src: Sources, limits: Limits) -> CheckResult:
    """Phase 7 check 8, outside Spark, over the window the serving run judged."""
    name = "silver_reconciles_with_audit"
    pointer = src.current_pointer()
    if pointer is None:
        return _result(name, False, "no serving pointer", "a pointer to anchor the window")
    as_of = _instant(pointer["as_of"])
    newest = as_of - timedelta(seconds=limits.reconciliation_settle_seconds)
    oldest = as_of - timedelta(seconds=limits.reconciliation_lookback_seconds)
    parsed = dict(src.query(
        "SELECT crawl_run_id, sum(parsed_count) FROM audit.crawl_request_attempt WHERE started_at <= %s GROUP BY 1",
        (as_of,),
    ))
    completed = dict(src.query(
        "SELECT crawl_run_id, completed_at FROM audit.crawl_run WHERE started_at <= %s", (as_of,),
    ))
    mismatched = []
    silver = _silver_rows_by_run(src, as_of=as_of, oldest=oldest)
    for run_id, observed in sorted(silver.items()):
        # As in check 8: an audited run settles at its completion, a run with
        # no audit at its last observation; an unfinished one waits.
        settled = _instant(completed[run_id]) if completed.get(run_id) is not None else (
            None if run_id in completed else max(observed))
        if settled is None or not (oldest <= settled <= newest):
            continue
        if int(parsed.get(run_id) or -1) != len(observed):
            mismatched.append({"crawl_run_id": run_id, "silver_rows": len(observed), "parsed": parsed.get(run_id)})
    return _result(name, not mismatched, {"as_of": as_of.isoformat(), "mismatched": mismatched[:10]}, {"mismatched": []})


def dlq_only_bad_records(src: Sources, limits: Limits) -> CheckResult:
    from config.marketplace_dlq import DlqStage

    allowed = {stage.value for stage in DlqStage}
    stages = Counter(src.dlq_stages())
    unexpected = sorted(stage for stage in stages if stage not in allowed)
    return _result("dlq_only_bad_records", not unexpected, dict(sorted(stages.items())), sorted(allowed))


def speed_last_batch_succeeded(src: Sources, limits: Limits) -> CheckResult:
    rows = src.query(
        "SELECT batch_id, status FROM audit.marketplace_speed_batch ORDER BY started_at DESC LIMIT 1")
    if not rows:
        return _result("speed_last_batch_succeeded", False, "no speed batch", "SUCCEEDED")
    batch_id, status = rows[0]
    return _result("speed_last_batch_succeeded", status == "SUCCEEDED", {"batch_id": batch_id, "status": status}, "SUCCEEDED")


def es_changes_unique(src: Sources, limits: Limits) -> CheckResult:
    from config.settings import ES_INDEX_MARKETPLACE_CHANGES

    ids = [doc.get("event_id") for doc in src.es_documents(ES_INDEX_MARKETPLACE_CHANGES)]
    distinct = len(set(ids))
    return _result("es_changes_unique", len(ids) == distinct and len(ids) > 0,
                   {"documents": len(ids), "distinct_event_ids": distinct}, "documents == distinct_event_ids > 0")


def redis_offer_state_present(src: Sources, limits: Limits) -> CheckResult:
    """Every offer whose state is younger than the Redis TTL, measured from the newest one."""
    from config.settings import ES_INDEX_MARKETPLACE_OFFERS

    docs = src.es_documents(ES_INDEX_MARKETPLACE_OFFERS)
    if not docs:
        return _result("redis_offer_state_present", False, "no offers in Elasticsearch", "at least one offer")
    newest = max(_instant(doc["observed_at"]) for doc in docs)
    live = sorted(doc["offer_id"] for doc in docs
                  if _instant(doc["observed_at"]) > newest - timedelta(seconds=limits.offer_ttl_seconds))
    missing = [offer for offer in live if not src.redis_exists(f"rt:offer:{offer}")]
    return _result("redis_offer_state_present", not missing, {"offers": len(live), "missing": missing[:10]}, {"missing": []})


def batch_latest_terminal(src: Sources, limits: Limits) -> CheckResult:
    rows = src.query(
        "SELECT r.run_id, r.status, (SELECT count(*) FROM audit.marketplace_quality_result q WHERE q.run_id = r.run_id) "
        "FROM audit.marketplace_batch_run r WHERE r.run_id LIKE %s ORDER BY r.as_of DESC LIMIT 1",
        (SCHEDULED_RUN_PREFIX + "%",),
    )
    if not rows:
        return _result("batch_latest_terminal", False, "no scheduled run", "SUCCEEDED, or QUALITY_FAILED with results")
    run_id, status, results = rows[0]
    ok = status == "SUCCEEDED" or (status == "QUALITY_FAILED" and results > 0)
    return _result("batch_latest_terminal", ok, {"run_id": run_id, "status": status, "quality_results": results},
                   "SUCCEEDED, or QUALITY_FAILED with results")


def pointer_matches_cache(src: Sources, limits: Limits) -> CheckResult:
    pointer = src.current_pointer()
    cache = src.query("SELECT run_id FROM audit.marketplace_cache_version")
    observed = {"pointer": pointer and pointer["run_id"], "cache": cache[0][0] if cache else None}
    return _result("pointer_matches_cache", observed["pointer"] is not None and observed["pointer"] == observed["cache"],
                   observed, "pointer == cache")


def pointer_gold_exists(src: Sources, limits: Limits) -> CheckResult:
    pointer = src.current_pointer()
    if pointer is None:
        return _result("pointer_gold_exists", False, "no serving pointer", "every dataset with its row count")
    wrong = []
    for dataset in pointer["datasets"]:
        rows = src.parquet_rows(dataset["uri"])
        if rows != dataset["row_count"]:
            wrong.append({"dataset": dataset["dataset_name"], "rows": rows, "recorded": dataset["row_count"]})
    return _result("pointer_gold_exists", not wrong, {"datasets": len(pointer["datasets"]), "wrong": wrong}, {"wrong": []})


def quality_results_complete(src: Sources, limits: Limits) -> CheckResult:
    from config.quality_rules import QUALITY_RULES

    pointer = src.current_pointer()
    if pointer is None:
        return _result("quality_results_complete", False, "no serving pointer", "one result per rule")
    stored = {name for (name,) in src.query(
        "SELECT check_name FROM audit.marketplace_quality_result WHERE run_id = %s", (pointer["run_id"],))}
    expected = {rule.check_name for rule in QUALITY_RULES}
    return _result("quality_results_complete", stored == expected,
                   {"run_id": pointer["run_id"], "missing": sorted(expected - stored), "unexpected": sorted(stored - expected)},
                   {"missing": [], "unexpected": []})


CHECKS: tuple[Callable[[Sources, Limits], CheckResult], ...] = (
    bronze_present, kafka_to_silver_lag, silver_reconciles_with_audit, dlq_only_bad_records,
    speed_last_batch_succeeded, es_changes_unique, redis_offer_state_present, batch_latest_terminal,
    pointer_matches_cache, pointer_gold_exists, quality_results_complete,
)
# source_health_projected (plan 9.2) arrives with the ops projector in WP8:
# until then the index it reads does not exist.


def validate(src: Sources, limits: Limits, checks: Iterable[Callable[[Sources, Limits], CheckResult]] = CHECKS) -> list[CheckResult]:
    results = []
    for check in checks:
        try:
            results.append(check(src, limits))
        except Exception as error:  # noqa: BLE001 - a check that cannot run has failed
            results.append(CheckResult(check.__name__, FAIL, f"{type(error).__name__}: {error}"[:500], "check ran"))
    return sorted(results, key=lambda result: result.check)


def report(results: Sequence[CheckResult]) -> str:
    return json.dumps({"passed": all(r.status == PASS for r in results), "checks": [asdict(r) for r in results]},
                      sort_keys=True, default=str, indent=2)


def default_limits() -> Limits:
    from config.settings import (
        MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS, MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS,
        REDIS_MARKETPLACE_OFFER_TTL_SECONDS,
    )
    return Limits(MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS, MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS,
                  REDIS_MARKETPLACE_OFFER_TTL_SECONDS)


# ---------------------------------------------------------------------------
# The real sources
# ---------------------------------------------------------------------------

class LiveSources:
    """PostgreSQL, the lake, Kafka, Elasticsearch and Redis, read-only."""

    def __init__(self) -> None:
        from common.postgres import postgres_connection_factory

        self._connect = postgres_connection_factory()

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return list(cur.fetchall())

    # -- lake: s3a:// through the MinIO client, file:// from the filesystem --
    @staticmethod
    def _split(uri: str) -> tuple[str, str, str]:
        parsed = urlparse(uri)
        return parsed.scheme, parsed.netloc, unquote(parsed.path.lstrip("/") if parsed.scheme != "file" else parsed.path)

    @staticmethod
    def _client():
        from common.object_store import _client
        from config.storage import active_profile

        return _client(active_profile())

    def object_exists(self, uri: str) -> bool:
        scheme, bucket, key = self._split(uri)
        if scheme == "file":
            from pathlib import Path
            return Path(key.lstrip("/") if key[2:3] == ":" else key).exists()
        from minio.error import S3Error
        try:
            self._client().stat_object(bucket, key)
            return True
        except S3Error as error:
            if error.code in ("NoSuchKey", "NoSuchBucket", "NoSuchObject"):
                return False
            raise

    def list_objects(self, uri_prefix: str) -> list[str]:
        scheme, bucket, key = self._split(uri_prefix)
        if scheme == "file":
            from pathlib import Path
            root = Path(key.lstrip("/") if key[2:3] == ":" else key)
            return sorted(path.as_uri() for path in root.rglob("*") if path.is_file())
        prefix = key.rstrip("/") + "/"
        return sorted(f"s3a://{bucket}/{obj.object_name}" for obj in self._client().list_objects(bucket, prefix=prefix, recursive=True))

    def read_object(self, uri: str) -> bytes | None:
        scheme, bucket, key = self._split(uri)
        if scheme == "file":
            from pathlib import Path
            path = Path(key.lstrip("/") if key[2:3] == ":" else key)
            return path.read_bytes() if path.exists() else None
        from minio.error import S3Error
        response = None
        try:
            response = self._client().get_object(bucket, key)
            return response.read()
        except S3Error as error:
            if error.code in ("NoSuchKey", "NoSuchBucket"):
                return None
            raise
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def parquet_rows(self, uri_prefix: str) -> int | None:
        """Total rows over a Spark output directory, or None without its _SUCCESS marker."""
        import io

        import pyarrow.parquet as pq

        objects = self.list_objects(uri_prefix)
        if not any(uri.endswith("/_SUCCESS") for uri in objects):
            return None
        return sum(pq.ParquetFile(io.BytesIO(self.read_object(uri))).metadata.num_rows
                   for uri in objects if uri.endswith(".parquet"))

    def current_pointer(self) -> dict | None:
        from batch_layer.marketplace_manifest import CURRENT_POINTER_PATH, GOLD_ZONE
        from common.object_store import get_bytes

        payload = get_bytes(GOLD_ZONE, CURRENT_POINTER_PATH)
        return None if payload is None else json.loads(payload)

    # -- Kafka --
    def consumer_lag(self, group: str, topic: str) -> int:
        from kafka import KafkaAdminClient, KafkaConsumer

        from config.settings import KAFKA_BOOTSTRAP_SERVERS

        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        try:
            committed = {tp: meta.offset for tp, meta in admin.list_consumer_group_offsets(group).items() if tp.topic == topic}
            from kafka import TopicPartition
            partitions = [TopicPartition(topic, p) for p in (consumer.partitions_for_topic(topic) or ())]
            ends = consumer.end_offsets(partitions)
            return sum(end - max(committed.get(tp, 0), 0) for tp, end in ends.items())
        finally:
            consumer.close()
            admin.close()

    def dlq_stages(self) -> list[str]:
        from kafka import KafkaConsumer, TopicPartition

        from config.settings import KAFKA_BOOTSTRAP_SERVERS
        from config.topics import MARKETPLACE_OBSERVATIONS_DLQ

        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, enable_auto_commit=False)
        try:
            topic = MARKETPLACE_OBSERVATIONS_DLQ.name
            partitions = [TopicPartition(topic, p) for p in (consumer.partitions_for_topic(topic) or ())]
            consumer.assign(partitions)
            consumer.seek_to_beginning(*partitions)
            ends = consumer.end_offsets(partitions)
            stages = []
            while any(consumer.position(tp) < end for tp, end in ends.items()):
                for records in consumer.poll(timeout_ms=2000).values():
                    stages.extend(json.loads(record.value).get("stage") for record in records)
            return stages
        finally:
            consumer.close()

    # -- Elasticsearch and Redis --
    def es_documents(self, index: str) -> list[dict]:
        from elasticsearch import Elasticsearch
        from elasticsearch.helpers import scan

        from config.settings import ES_HOST

        es = Elasticsearch(ES_HOST)
        try:
            if not es.indices.exists(index=index):
                return []
            return [hit["_source"] for hit in scan(es, index=index, query={"query": {"match_all": {}}})]
        finally:
            es.close()

    def redis_exists(self, key: str) -> bool:
        from redis import Redis

        from config.settings import REDIS_DB, REDIS_HOST, REDIS_PORT

        client = getattr(self, "_redis", None)
        if client is None:
            client = self._redis = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        return bool(client.exists(key))

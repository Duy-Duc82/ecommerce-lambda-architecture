"""Backup, export and restore — Phase 8 plan section 12.

What is backed up, and why (plan 12.1): Bronze and Silver whole, because raw
truth cannot be recreated and canonical observations only by replaying a Kafka
topic whose retention does not keep them; the Gold *manifests* and
``current.json``, because they are the single definition of the serving
version; and the one Gold run ``current.json`` names, because a pointer that
does not resolve after a restore is not a pointer. Other Gold runs are
rebuildable from Silver. Kafka, Elasticsearch, Redis and the speed checkpoints
are derived or transient — the runbook says so rather than pretending they
were restored.

Three rules shape the code:

* **A backup is taken under the batch lock.** Not to freeze Bronze and Silver
  — they are append-only, so a copy taken seconds early is merely older, never
  torn — but so that no batch can promote a new pointer between reading
  ``current.json`` and dumping the cache that must agree with it. The lock
  belongs to the session that took it, which is why ``pg_dump`` runs in this
  process and not in a second container.
* **The pointer's ``run_id`` and ``audit.marketplace_cache_version.run_id``
  must be equal.** A backup whose pointer and cache disagree restores into a
  state ``validate`` rejects, so it is refused — before anything is copied,
  not after.
* **``current.json`` is written last on restore, and nothing is written at
  all until every SHA-256 verifies.** A pointer that arrives before the data
  it names is a window in which the stack is confidently serving nothing.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, ContextManager, Iterator, Protocol, Sequence
from urllib.parse import unquote, urlparse

BACKUP_SCHEMA_VERSION = "marketplace-backup.v1"
MANIFEST_NAME = "backup-manifest.json"
# audit holds the run history, quality evidence, crawl audit and frontier;
# cache holds what Superset serves. staging is rebuilt by every publish.
POSTGRES_SCHEMAS = ("audit", "cache")
# Gold first: the pointer and the run it names are what a restore must be able
# to resolve, and a backup that dies halfway is more useful having copied them.
LAKE_ZONES = ("gold", "bronze", "silver")
# How many restored raw artifacts are reparsed. Enough to catch a systematic
# corruption, few enough to keep a restore check minutes rather than hours.
DEFAULT_REPARSE_SAMPLE = 5


class BackupRefused(RuntimeError):
    """The stack is not in a state worth backing up, so nothing was copied."""


class RestoreRefused(RuntimeError):
    """The backup does not verify, so nothing was written."""


# ---------------------------------------------------------------------------
# Seams
# ---------------------------------------------------------------------------


class Lake(Protocol):
    def list_keys(self, zone: str, prefix: str = "") -> list[str]: ...
    def get(self, zone: str, key: str) -> bytes: ...
    def put(self, zone: str, key: str, data: bytes) -> None: ...


class Database(Protocol):
    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]: ...
    def dump_schema(self, schema: str, destination: Path) -> None: ...
    def restore_schema(self, schema: str, source: Path) -> None: ...
    def batch_lock(self) -> ContextManager[None]: ...


@dataclass(frozen=True)
class BackupStore:
    """Where a backup's bytes live: a directory both Compose projects can see."""

    root: Path

    def path(self, relative: str) -> Path:
        return self.root.joinpath(*relative.split("/"))

    def write(self, relative: str, data: bytes) -> None:
        target = self.path(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def read(self, relative: str) -> bytes:
        return self.path(relative).read_bytes()

    def exists(self, relative: str) -> bool:
        return self.path(relative).exists()


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pointer_key() -> str:
    from batch_layer.marketplace_manifest import CURRENT_POINTER_PATH

    return CURRENT_POINTER_PATH


def zone_key(uri: str, zone: str) -> str:
    """The key of ``uri`` relative to its zone root, for either lake profile."""
    from config.settings import data_lake_uri

    base = data_lake_uri(zone).rstrip("/")
    if uri.startswith(base):
        return unquote(uri[len(base):].lstrip("/"))
    # A manifest written under another profile still names the same object;
    # fall back to the path, minus the bucket for an s3a URI.
    parsed = urlparse(uri)
    path = unquote(parsed.path).lstrip("/")
    return path


def backup_id_for(moment: datetime) -> str:
    return "bk-" + moment.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sample_uris(uris: Sequence[str], count: int) -> list[str]:
    """A deterministic, spread-out sample: sort, then take evenly spaced ones.

    Deterministic so two people checking the same backup check the same
    artifacts, and spread so the sample is not all from one crawl run.
    """
    ordered = sorted(set(uris))
    if count <= 0 or not ordered:
        return []
    if len(ordered) <= count:
        return ordered
    step = len(ordered) / count
    return [ordered[int(index * step)] for index in range(count)]


def verify_entries(manifest: dict, store: BackupStore) -> list[str]:
    """Every file whose bytes do not match the manifest, by relative path."""
    bad = []
    for entry in manifest["files"]:
        relative = entry["path"]
        if not store.exists(relative):
            bad.append(f"{relative}: missing")
            continue
        payload = store.read(relative)
        if len(payload) != entry["size"]:
            bad.append(f"{relative}: size {len(payload)} != {entry['size']}")
        elif sha256(payload) != entry["sha256"]:
            bad.append(f"{relative}: sha256 mismatch")
    return bad


def restore_plan(manifest: dict) -> tuple[list[dict], list[dict], dict]:
    """``(postgres, lake, pointer)`` — the order a restore must write in.

    The pointer comes out of the lake list and back in last. Writing it
    earlier opens a window where the stack confidently serves a version whose
    data has not landed.
    """
    postgres = [entry for entry in manifest["files"] if "schema" in entry]
    lake = [entry for entry in manifest["files"] if "zone" in entry]
    pointers = [entry for entry in lake if entry["key"] == manifest["pointer_key"]]
    if len(pointers) != 1:
        raise RestoreRefused(f"the backup names {len(pointers)} pointer objects, expected exactly one")
    pointer = pointers[0]
    return postgres, [entry for entry in lake if entry is not pointer], pointer


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------


def _cache_run_id(db: Database) -> str | None:
    rows = db.query("SELECT run_id FROM audit.marketplace_cache_version")
    return rows[0][0] if rows else None


def backup(
    lake: Lake,
    db: Database,
    store: BackupStore,
    *,
    backup_id: str | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    tools: dict[str, str] | None = None,
    log: Callable[[str], None] = lambda line: None,
) -> dict:
    """Copy the stack into ``store`` and return the backup manifest."""
    from batch_layer.marketplace_manifest import parse_manifest
    from config.settings import MARKETPLACE_GOLD_DATASET

    started = clock()
    identifier = backup_id or backup_id_for(started)
    key_of_pointer = pointer_key()

    with db.batch_lock():
        payload = lake.get("gold", key_of_pointer)
        if payload is None:
            raise BackupRefused("there is no serving pointer, so there is no version to back up")
        pointer = parse_manifest(payload)
        cache_run_id = _cache_run_id(db)
        if cache_run_id != pointer.run_id:
            # Checked before copying rather than after: a backup that cannot
            # restore into a state validate accepts is not worth the bytes.
            raise BackupRefused(
                f"the pointer serves {pointer.run_id} but the cache holds {cache_run_id}; "
                "a backup of the two disagreeing restores into a state validate rejects")

        files: list[dict] = []
        counts: dict[str, int] = {}

        def copy(zone: str, key: str) -> None:
            data = lake.get(zone, key)
            if data is None:
                raise BackupRefused(f"{zone}/{key} is named by the stack but absent from it")
            relative = f"lake/{zone}/{key}"
            store.write(f"{identifier}/{relative}", data)
            files.append({"path": relative, "zone": zone, "key": key,
                          "size": len(data), "sha256": sha256(data)})
            counts[zone] = counts.get(zone, 0) + 1

        # 1. the pointer, 2. the Gold run it names, 3. the manifests.
        copy("gold", key_of_pointer)
        run_prefix = zone_key(pointer.gold_run_uri, "gold").rstrip("/") + "/"
        for key in lake.list_keys("gold", run_prefix):
            copy("gold", key)
        for key in lake.list_keys("gold", f"{MARKETPLACE_GOLD_DATASET}/manifests/"):
            copy("gold", key)
        log(f"gold: {counts.get('gold', 0)} objects")

        # 4. PostgreSQL, while the lock still holds.
        for schema in POSTGRES_SCHEMAS:
            relative = f"postgres/{schema}.dump"
            destination = store.path(f"{identifier}/{relative}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            db.dump_schema(schema, destination)
            data = destination.read_bytes()
            files.append({"path": relative, "schema": schema,
                          "size": len(data), "sha256": sha256(data)})
            counts["postgres"] = counts.get("postgres", 0) + 1
            log(f"pg_dump {schema}: {len(data)} bytes")

        # 5. Bronze and Silver, whole.
        for zone in ("bronze", "silver"):
            for key in lake.list_keys(zone):
                copy(zone, key)
            log(f"{zone}: {counts.get(zone, 0)} objects")

        manifest = {
            "schema_version": BACKUP_SCHEMA_VERSION,
            "backup_id": identifier,
            "created_at": started.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "pointer_run_id": pointer.run_id,
            "cache_run_id": cache_run_id,
            "pointer_as_of": pointer.as_of.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "pointer_key": key_of_pointer,
            "datasets": [{"dataset_name": dataset.dataset_name, "uri": dataset.uri,
                          "row_count": dataset.row_count} for dataset in pointer.datasets],
            "counts": dict(sorted(counts.items())),
            "tools": dict(sorted((tools or {}).items())),
            "files": files,
        }
        store.write(f"{identifier}/{MANIFEST_NAME}", _manifest_bytes(manifest))
        return manifest


def _manifest_bytes(manifest: dict) -> bytes:
    return (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def read_backup_manifest(store: BackupStore, backup_id: str) -> dict:
    relative = f"{backup_id}/{MANIFEST_NAME}"
    if not store.exists(relative):
        raise RestoreRefused(f"no {MANIFEST_NAME} under {backup_id}")
    manifest = json.loads(store.read(relative))
    if manifest.get("schema_version") != BACKUP_SCHEMA_VERSION:
        raise RestoreRefused(f"unsupported backup schema version: {manifest.get('schema_version')!r}")
    return manifest


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------


def restore(
    lake: Lake,
    db: Database,
    store: BackupStore,
    *,
    backup_id: str,
    log: Callable[[str], None] = lambda line: None,
) -> dict:
    """Load a backup into an empty stack. Verifies everything before writing."""
    manifest = read_backup_manifest(store, backup_id)
    scoped = BackupStore(store.path(backup_id))
    bad = verify_entries(manifest, scoped)
    if bad:
        # Before anything is written: a half-restored stack is worse than one
        # that was never touched, because it looks restored.
        raise RestoreRefused(f"{len(bad)} file(s) failed verification: {bad[:5]}")
    log(f"verified {len(manifest['files'])} files")

    postgres, lake_entries, pointer = restore_plan(manifest)
    for entry in postgres:
        db.restore_schema(entry["schema"], scoped.path(entry["path"]))
        log(f"pg_restore {entry['schema']}")
    for entry in lake_entries:
        lake.put(entry["zone"], entry["key"], scoped.read(entry["path"]))
    log(f"lake: {len(lake_entries)} objects")
    # Last, always.
    lake.put(pointer["zone"], pointer["key"], scoped.read(pointer["path"]))
    log("pointer written")
    return manifest


# ---------------------------------------------------------------------------
# Restore checks (plan 12.3 step 3)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckResult:
    check: str
    status: str
    observed: Any
    expected: Any


PASS, FAIL = "PASS", "FAIL"


def _result(name: str, ok: bool, observed: Any, expected: Any) -> CheckResult:
    return CheckResult(name, PASS if ok else FAIL, observed, expected)


def check_pointer_matches_cache(lake: Lake, db: Database, manifest: dict) -> CheckResult:
    from batch_layer.marketplace_manifest import parse_manifest

    payload = lake.get("gold", manifest["pointer_key"])
    restored = None if payload is None else parse_manifest(payload).run_id
    cached = _cache_run_id(db)
    return _result("restored_pointer_matches_cache", restored is not None and restored == cached,
                   {"pointer": restored, "cache": cached}, {"equal": True})


def check_datasets_present(lake: Lake, manifest: dict, *, rows_at: Callable[[str], int | None]) -> CheckResult:
    """Every dataset the restored pointer names exists, with its manifest row count."""
    observed, wrong = {}, []
    for dataset in manifest["datasets"]:
        count = rows_at(dataset["uri"])
        observed[dataset["dataset_name"]] = count
        if count != dataset["row_count"]:
            wrong.append({"dataset": dataset["dataset_name"], "rows": count,
                          "manifest_rows": dataset["row_count"]})
    return _result("restored_datasets_match_manifest", not wrong, {"rows": observed, "wrong": wrong},
                   {"rows": {d["dataset_name"]: d["row_count"] for d in manifest["datasets"]}})


# Only attempts that put every observation they parsed into the pipeline.
#
# Found by running the restore for real on 2026-10-04: the sample drew attempt
# 380, FAILED/PUBLISH_ERROR with parsed_count 0 — drill D2's deliberate
# outcome, where the fetch and the parse succeeded, the raw body reached
# Bronze, and Kafka was down so nothing reached Silver. Reparsing that
# artifact reports NEW_OBSERVATIONS, correctly, and the restore check failed
# on a faithful copy.
#
# ``parsed_count`` is the *acknowledged* count, so a partial publish also
# leaves observations Silver will never have; the audit cannot tell a partial
# publish from a complete one by count alone, but a partial one is always
# FAILED with an error_kind, and that it can.
SAMPLEABLE_ATTEMPTS_SQL = (
    "SELECT raw_uri FROM audit.crawl_request_attempt "
    "WHERE raw_uri IS NOT NULL AND error_kind IS NULL AND parsed_count > 0"
)


def check_reparse_sample(db: Database, *, sample_size: int, reparse: Callable[[list[str]], dict]) -> CheckResult:
    """``crawler.reparse`` on a deterministic sample of restored raw artifacts."""
    rows = db.query(SAMPLEABLE_ATTEMPTS_SQL)
    chosen = sample_uris([uri for (uri,) in rows], sample_size)
    if not chosen:
        # Not a pass: a restore whose audit names no fully published artifact
        # has nothing to prove the Bronze and Silver copies against.
        return _result("restored_raw_reparses_identical", False,
                       "no fully published raw artifact in the restored audit",
                       f"{sample_size} artifacts to reparse")
    summary = reparse(chosen)
    counts = summary.get("counts", {})
    from crawler.reparse import IDENTICAL

    ok = set(counts) == {IDENTICAL}
    return _result("restored_raw_reparses_identical", ok,
                   {"sampled": len(chosen), "counts": dict(sorted(counts.items())),
                    "problems": summary.get("problems", [])[:3]},
                   {"counts": {IDENTICAL: len(chosen)}})


# The three stores plan 12.1 deliberately does not back up: Kafka,
# Elasticsearch and Redis, all derived or transient. A stack that has just
# been restored and has not crawled yet therefore has all three empty, and
# exactly these `ops validate` checks fail on it:
#
#   es_changes_unique          0 documents, because nothing has streamed yet;
#   redis_offer_state_present  no offer state, for the same reason;
#   kafka_to_silver_lag        the broker is new, so the Silver consumer group
#                              has never existed and asking for its offsets
#                              raises GroupCoordinatorNotAvailableError. Seen
#                              for real on 2026-10-04.
#
# Tolerating these is plan 12.3 step 4 — "ES and Redis start empty and refill
# from new crawls" — read honestly. Tolerating anything else would hide a real
# defect, so the list is exactly these three and the restore fails on a fourth.
RESTORED_STACK_EXPECTED_EMPTY = ("es_changes_unique", "kafka_to_silver_lag", "redis_offer_state_present")


def judge_restored_validate(results: Sequence[Any]) -> tuple[bool, list[str], list[str]]:
    """``(ok, tolerated, unexpected)`` for ``ops validate`` on a restored stack."""
    failed = [result.check for result in results if result.status != PASS]
    tolerated = sorted(name for name in failed if name in RESTORED_STACK_EXPECTED_EMPTY)
    unexpected = sorted(name for name in failed if name not in RESTORED_STACK_EXPECTED_EMPTY)
    return not unexpected, tolerated, unexpected


def report(results: Sequence[CheckResult]) -> str:
    ordered = sorted(results, key=lambda result: result.check)
    return json.dumps({"passed": all(result.status == PASS for result in ordered),
                       "checks": [result.__dict__ for result in ordered]},
                      ensure_ascii=False, indent=2, sort_keys=True, default=str)


# ---------------------------------------------------------------------------
# Live wiring
# ---------------------------------------------------------------------------


class LiveLake:
    """The medallion zones, through MinIO or the local filesystem."""

    def list_keys(self, zone: str, prefix: str = "") -> list[str]:
        from config.settings import DATA_LAKE_LOCAL_ROOT, MINIO_BUCKETS
        from config.storage import active_profile

        profile = active_profile()
        if profile.is_local:
            root = DATA_LAKE_LOCAL_ROOT / zone.lower()
            base = root / prefix if prefix else root
            if not base.exists():
                return []
            return sorted(path.relative_to(root).as_posix() for path in base.rglob("*") if path.is_file())
        from common.object_store import _client

        client = _client(profile)
        bucket = MINIO_BUCKETS[zone.lower()]
        if not client.bucket_exists(bucket):
            return []
        return sorted(obj.object_name for obj in client.list_objects(bucket, prefix=prefix or None, recursive=True))

    def get(self, zone: str, key: str) -> bytes | None:
        from common.object_store import get_bytes

        return get_bytes(zone, key)

    def put(self, zone: str, key: str, data: bytes) -> None:
        from common.object_store import put_bytes

        put_bytes(zone, key, data)


class LivePostgres:
    """PostgreSQL, plus the pg_dump and pg_restore that ship in the ops image."""

    def _dsn_env(self) -> dict[str, str]:
        from config.settings import POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER

        return {**os.environ, "PGHOST": POSTGRES_HOST, "PGPORT": str(POSTGRES_PORT),
                "PGUSER": POSTGRES_USER, "PGPASSWORD": POSTGRES_PASSWORD}

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        from common.postgres import postgres_connection_factory

        with postgres_connection_factory()() as conn, conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return list(cur.fetchall())

    def dump_schema(self, schema: str, destination: Path) -> None:
        from config.settings import POSTGRES_DB

        subprocess.run(["pg_dump", "--format=custom", "--no-owner", "--no-privileges",
                        f"--schema={schema}", "--file", str(destination), POSTGRES_DB],
                       env=self._dsn_env(), check=True)

    def restore_schema(self, schema: str, source: Path) -> None:
        from config.settings import POSTGRES_DB

        # --clean --if-exists so this works both into a database the Compose
        # init script already populated and into an empty one.
        subprocess.run(["pg_restore", "--no-owner", "--no-privileges", "--clean", "--if-exists",
                        "--exit-on-error", "--dbname", POSTGRES_DB, str(source)],
                       env=self._dsn_env(), check=True)

    @contextmanager
    def batch_lock(self) -> Iterator[None]:
        from batch_layer.marketplace_lock import exclusive_batch
        from common.postgres import postgres_connection_factory

        with exclusive_batch(postgres_connection_factory()):
            yield


def tool_versions() -> dict[str, str]:
    import sys

    def version(command: str) -> str:
        try:
            return subprocess.run([command, "--version"], capture_output=True, text=True,
                                  check=True).stdout.strip()
        except Exception as error:  # pragma: no cover - environment, not logic
            return f"unavailable: {type(error).__name__}"

    return {"backup_schema": BACKUP_SCHEMA_VERSION, "pg_dump": version("pg_dump"),
            "pg_restore": version("pg_restore"), "python": sys.version.split()[0]}


def parquet_rows_at(uri: str) -> int | None:
    """Rows over a Spark output directory, or None without its _SUCCESS marker."""
    import io

    import pyarrow.parquet as pq

    lake = LiveLake()
    zone = "gold"
    prefix = zone_key(uri, zone).rstrip("/") + "/"
    keys = lake.list_keys(zone, prefix)
    if not any(key.endswith("/_SUCCESS") for key in keys):
        return None
    return sum(pq.ParquetFile(io.BytesIO(lake.get(zone, key))).metadata.num_rows
               for key in keys if key.endswith(".parquet"))


def reparse_sample(uris: Sequence[str]) -> dict:
    """Reparse the sampled artifacts against restored Silver, and summarise."""
    import common.object_store as object_store
    from config.settings import MARKETPLACE_SILVER_DATASET
    from crawler.reparse import RawArtifactRef, reparse_batch, summarise
    from crawler.sites.tiki import TikiCrawler

    adapters = {"tiki": TikiCrawler}
    refs = [RawArtifactRef.from_uri(uri) for uri in uris]
    outcomes = reparse_batch(
        refs,
        # Parse-only: no categories, no robots fetch, so a restore check never
        # reaches the marketplace.
        adapter_for=lambda code: adapters[code](categories=[], fetch_robots=False),
        reader=object_store.get_bytes,
        clock=lambda: datetime.now(timezone.utc),
        silver_reader=object_store.get_bytes,
        silver_dataset=MARKETPLACE_SILVER_DATASET,
    )
    return summarise(outcomes, reported_at=datetime.now(timezone.utc))


def restore_checks(lake: Lake, db: Database, manifest: dict, *, sample_size: int,
                   rows_at: Callable[[str], int | None] = parquet_rows_at,
                   reparse: Callable[[list[str]], dict] = reparse_sample) -> list[CheckResult]:
    """Plan 12.3 step 3, minus the quality-only batch, which needs Spark.

    ``mp restore`` drives that fourth check from the host, the same way it
    drives every other Spark job, and prints its verdict next to these.
    """
    checks = []
    for name, run in (("pointer", lambda: check_pointer_matches_cache(lake, db, manifest)),
                      ("datasets", lambda: check_datasets_present(lake, manifest, rows_at=rows_at)),
                      ("reparse", lambda: check_reparse_sample(db, sample_size=sample_size, reparse=reparse))):
        try:
            checks.append(run())
        except Exception as error:
            checks.append(CheckResult(f"restore_check_{name}", FAIL,
                                      f"{type(error).__name__}: {error}"[:500], "no error"))
    return checks

"""Phase 8 plan section 12: backup, export and restore (tests 24-26).

Offline. The lake is a dict, PostgreSQL is a fake that records what it was
asked to dump and restore, and ``pg_dump`` never runs — what is asserted here
is the ordering and the verification, which is where a backup goes wrong.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from batch_layer.marketplace_manifest import (
    CURRENT_POINTER_PATH,
    GoldManifest,
    ManifestDataset,
    serialize_manifest,
)
from config.settings import MARKETPLACE_MANIFEST_SCHEMA_VERSION
from ops import backup as b

AS_OF = datetime(2026, 10, 4, 7, tzinfo=timezone.utc)
RUN_ID = "mp-20261004T0700Z"
GOLD_RUN_URI = "s3a://ecommerce-gold/marketplace/run_id=mp-20261004T0700Z"
RAW_URI = ("s3a://ecommerce-bronze/marketplace/raw/marketplace=tiki/observed_date=2026-10-04/"
           "hour=06/crawl_run_id=crawl-1/raw_artifact_id=raw-1/body.bin")


def _pointer(run_id: str = RUN_ID) -> bytes:
    manifest = GoldManifest(
        manifest_schema_version=MARKETPLACE_MANIFEST_SCHEMA_VERSION,
        run_id=run_id,
        as_of=AS_OF,
        silver_uri="s3a://ecommerce-silver/marketplace/offer_observations",
        gold_run_uri=GOLD_RUN_URI,
        rule_versions={"quality": "quality-rules.v2"},
        datasets=(ManifestDataset("offer_current", f"{GOLD_RUN_URI}/offer_current", 12, ()),),
        quality={"status": "PASSED"},
        previous_run_id=None,
    )
    return serialize_manifest(manifest)


class FakeLake:
    def __init__(self, objects=None):
        self.objects: dict[tuple[str, str], bytes] = dict(objects or {})
        self.puts: list[tuple[str, str]] = []

    def list_keys(self, zone, prefix=""):
        return sorted(key for (z, key) in self.objects if z == zone and key.startswith(prefix))

    def get(self, zone, key):
        return self.objects.get((zone, key))

    def put(self, zone, key, data):
        self.objects[(zone, key)] = data
        self.puts.append((zone, key))


class FakeDb:
    """``attempts`` are ``(raw_uri, error_kind, parsed_count)``, as the audit holds them."""

    def __init__(self, *, cache_run_id=RUN_ID, attempts=((RAW_URI, None, 7),)):
        self.cache_run_id = cache_run_id
        self.attempts = list(attempts)
        self.dumped: list[str] = []
        self.restored: list[str] = []
        self.lock_held = 0
        self.events: list[str] = []

    def query(self, sql, params=()):
        if "marketplace_cache_version" in sql:
            return [(self.cache_run_id,)] if self.cache_run_id else []
        if "crawl_request_attempt" in sql:
            # The real column predicate, applied here so the test exercises
            # which attempts are sampled rather than how the SQL is spelled.
            return [(uri,) for uri, error_kind, parsed in self.attempts
                    if uri is not None
                    and ("error_kind IS NULL" not in sql or error_kind is None)
                    and ("parsed_count > 0" not in sql or parsed > 0)]
        raise AssertionError(f"unexpected query: {sql}")

    def dump_schema(self, schema, destination):
        assert self.lock_held, "a dump taken outside the batch lock can disagree with the pointer"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(f"PGDMP {schema}".encode())
        self.dumped.append(schema)
        self.events.append(f"dump:{schema}")

    def restore_schema(self, schema, source):
        self.restored.append(schema)
        self.events.append(f"restore:{schema}")

    @contextmanager
    def batch_lock(self):
        self.lock_held += 1
        self.events.append("lock")
        try:
            yield
        finally:
            self.lock_held -= 1
            self.events.append("unlock")


def _full_lake() -> FakeLake:
    return FakeLake({
        ("gold", CURRENT_POINTER_PATH): _pointer(),
        ("gold", "marketplace/run_id=mp-20261004T0700Z/offer_current/part-0.parquet"): b"parquet",
        ("gold", "marketplace/run_id=mp-20261004T0700Z/offer_current/_SUCCESS"): b"",
        ("gold", "marketplace/manifests/run_id=mp-20261004T0700Z/manifest.json"): _pointer(),
        ("gold", "marketplace/run_id=mp-20260101T0000Z/offer_current/part-0.parquet"): b"old run",
        ("bronze", "marketplace/raw/marketplace=tiki/observed_date=2026-10-04/hour=06/"
                   "crawl_run_id=crawl-1/raw_artifact_id=raw-1/body.bin"): b"raw body",
        ("silver", "marketplace/offer_observations/marketplace=tiki/observed_date=2026-10-04/o1.json"): b"{}",
    })


def _backup(tmp_path, lake=None, db=None, **kwargs):
    lake = lake or _full_lake()
    db = db or FakeDb()
    store = b.BackupStore(tmp_path)
    manifest = b.backup(lake, db, store, backup_id="bk-test", clock=lambda: AS_OF, **kwargs)
    return manifest, lake, db, b.BackupStore(tmp_path / "bk-test")


# --- test 24: every copied object is hashed, and one changed byte is fatal ---

def test_the_manifest_records_a_sha256_and_a_size_for_every_copied_object(tmp_path):
    manifest, _, _, scoped = _backup(tmp_path)

    assert manifest["files"]
    for entry in manifest["files"]:
        payload = scoped.read(entry["path"])
        assert entry["sha256"] == b.sha256(payload)
        assert entry["size"] == len(payload)
    assert b.verify_entries(manifest, scoped) == []


def test_a_single_changed_byte_fails_verification_and_writes_nothing(tmp_path):
    manifest, _, _, scoped = _backup(tmp_path)
    victim = next(entry for entry in manifest["files"] if entry.get("zone") == "silver")
    corrupted = bytearray(scoped.read(victim["path"]))
    corrupted[0] ^= 0x01
    scoped.path(victim["path"]).write_bytes(bytes(corrupted))

    target_lake, target_db = FakeLake(), FakeDb()
    with pytest.raises(b.RestoreRefused, match="failed verification"):
        b.restore(target_lake, target_db, b.BackupStore(tmp_path), backup_id="bk-test")

    # Nothing at all, not "everything up to the bad file": a half-restored
    # stack is worse than an untouched one, because it looks restored.
    assert target_lake.puts == [] and target_db.restored == []


def test_a_missing_file_fails_verification_too(tmp_path):
    manifest, _, _, scoped = _backup(tmp_path)
    scoped.path(manifest["files"][0]["path"]).unlink()

    with pytest.raises(b.RestoreRefused, match="failed verification"):
        b.restore(FakeLake(), FakeDb(), b.BackupStore(tmp_path), backup_id="bk-test")


def test_a_truncated_file_of_the_same_prefix_is_caught_by_size(tmp_path):
    manifest, _, _, scoped = _backup(tmp_path)
    victim = next(entry for entry in manifest["files"] if entry["size"] > 1)
    scoped.path(victim["path"]).write_bytes(scoped.read(victim["path"])[:1])

    assert any("size" in problem for problem in b.verify_entries(manifest, scoped))


# --- test 25: a backup whose pointer and cache disagree is refused -----------

def test_a_backup_is_refused_when_the_pointer_and_the_cache_name_different_runs(tmp_path):
    db = FakeDb(cache_run_id="mp-20261003T0700Z")

    with pytest.raises(b.BackupRefused, match="validate rejects"):
        _backup(tmp_path, db=db)

    # Refused before the dump, not after: the bytes are never written.
    assert db.dumped == []
    assert list(tmp_path.iterdir()) == []


def test_a_backup_is_refused_when_the_cache_has_no_version_at_all(tmp_path):
    with pytest.raises(b.BackupRefused):
        _backup(tmp_path, db=FakeDb(cache_run_id=None))


def test_a_backup_is_refused_when_there_is_no_serving_pointer(tmp_path):
    lake = _full_lake()
    del lake.objects[("gold", CURRENT_POINTER_PATH)]

    with pytest.raises(b.BackupRefused, match="no serving pointer"):
        _backup(tmp_path, lake=lake)


def test_the_two_run_ids_are_both_recorded_so_the_check_is_auditable(tmp_path):
    manifest, _, _, _ = _backup(tmp_path)

    assert manifest["pointer_run_id"] == manifest["cache_run_id"] == RUN_ID


# --- test 26: restore writes current.json last -------------------------------

def test_restore_writes_the_pointer_last_and_postgres_before_the_lake(tmp_path):
    _backup(tmp_path)
    lake, db = FakeLake(), FakeDb()

    b.restore(lake, db, b.BackupStore(tmp_path), backup_id="bk-test")

    assert lake.puts[-1] == ("gold", CURRENT_POINTER_PATH)
    assert CURRENT_POINTER_PATH not in [key for _, key in lake.puts[:-1]]
    # PostgreSQL first: the cache must already name the run the pointer will.
    assert db.restored == list(b.POSTGRES_SCHEMAS)


def test_a_backup_naming_no_pointer_object_is_refused_rather_than_guessed(tmp_path):
    manifest, _, _, scoped = _backup(tmp_path)
    manifest["pointer_key"] = "marketplace/not-the-pointer.json"

    with pytest.raises(b.RestoreRefused, match="pointer objects"):
        b.restore_plan(manifest)


def test_every_backed_up_object_comes_back(tmp_path):
    _, source, _, _ = _backup(tmp_path)
    target = FakeLake()

    manifest = b.restore(target, FakeDb(), b.BackupStore(tmp_path), backup_id="bk-test")

    restored = {(entry["zone"], entry["key"]) for entry in manifest["files"] if "zone" in entry}
    assert set(target.objects) == restored
    for zone, key in restored:
        assert target.objects[(zone, key)] == source.objects[(zone, key)]


# --- what is and is not copied (plan 12.1) -----------------------------------

def test_bronze_and_silver_are_copied_whole(tmp_path):
    manifest, lake, _, _ = _backup(tmp_path)
    copied = {(entry["zone"], entry["key"]) for entry in manifest["files"] if "zone" in entry}

    for zone in ("bronze", "silver"):
        assert {(z, key) for (z, key) in lake.objects if z == zone} <= copied


def test_only_the_gold_run_the_pointer_names_is_copied(tmp_path):
    manifest, _, _, _ = _backup(tmp_path)
    gold = {entry["key"] for entry in manifest["files"] if entry.get("zone") == "gold"}

    assert any("run_id=mp-20261004T0700Z" in key for key in gold)
    # Other Gold runs are rebuildable from Silver, so they are not backed up.
    assert not any("run_id=mp-20260101T0000Z" in key for key in gold)


def test_the_pointer_is_the_first_object_copied(tmp_path):
    manifest, _, _, _ = _backup(tmp_path)
    lake_entries = [entry for entry in manifest["files"] if "zone" in entry]

    assert lake_entries[0]["key"] == CURRENT_POINTER_PATH


def test_both_postgres_schemas_are_dumped_under_the_lock(tmp_path):
    _, _, db, _ = _backup(tmp_path)

    assert db.dumped == list(b.POSTGRES_SCHEMAS)
    # The dump asserts the lock itself; this pins that it was taken and released.
    assert db.events[0] == "lock" and db.events[-1] == "unlock"


def test_the_manifest_counts_what_it_copied(tmp_path):
    manifest, _, _, _ = _backup(tmp_path)

    assert manifest["counts"]["postgres"] == len(b.POSTGRES_SCHEMAS)
    assert manifest["counts"]["bronze"] == 1 and manifest["counts"]["silver"] == 1
    assert sum(manifest["counts"].values()) == len(manifest["files"])


def test_the_manifest_records_the_tool_versions_it_was_taken_with(tmp_path):
    manifest, _, _, _ = _backup(tmp_path, tools={"pg_dump": "pg_dump (PostgreSQL) 18.6"})

    assert manifest["tools"]["pg_dump"].startswith("pg_dump")


def test_the_backup_id_is_the_utc_instant_it_started():
    assert b.backup_id_for(AS_OF) == "bk-20261004T070000Z"


def test_an_unknown_backup_schema_version_is_refused(tmp_path):
    store = b.BackupStore(tmp_path)
    store.write(f"bk-x/{b.MANIFEST_NAME}", json.dumps({"schema_version": "something-else"}).encode())

    with pytest.raises(b.RestoreRefused, match="unsupported backup schema version"):
        b.read_backup_manifest(store, "bk-x")


def test_an_object_the_stack_names_but_does_not_have_fails_the_backup(tmp_path):
    lake = _full_lake()
    del lake.objects[("gold", "marketplace/manifests/run_id=mp-20261004T0700Z/manifest.json")]
    lake.get = lambda zone, key: (None if key.endswith("manifest.json") and "manifests" in key
                                  else FakeLake.get(lake, zone, key))
    # Removing it from the listing too would simply copy less; the case that
    # matters is a key that lists and then cannot be read.
    lake.objects[("gold", "marketplace/manifests/run_id=mp-20261004T0700Z/manifest.json")] = b"x"

    with pytest.raises(b.BackupRefused, match="absent from it"):
        _backup(tmp_path, lake=lake)


# --- the deterministic reparse sample ----------------------------------------

def test_the_reparse_sample_is_deterministic_and_spread_out():
    uris = [f"s3a://b/raw_artifact_id=raw-{index:02d}" for index in range(20)]

    first = b.sample_uris(uris, 5)
    second = b.sample_uris(list(reversed(uris)), 5)

    assert first == second, "two people checking the same backup must check the same artifacts"
    assert len(first) == 5
    # Spread, not the first five: a sample all from one crawl run proves less.
    assert first != sorted(uris)[:5]


def test_a_sample_larger_than_the_population_takes_everything():
    uris = ["s3a://b/a", "s3a://b/b"]

    assert b.sample_uris(uris, 10) == sorted(uris)
    assert b.sample_uris([], 10) == []


# --- the restore checks (plan 12.3 step 3) -----------------------------------

def _restored(tmp_path):
    _backup(tmp_path)
    lake, db = FakeLake(), FakeDb()
    manifest = b.restore(lake, db, b.BackupStore(tmp_path), backup_id="bk-test")
    return lake, db, manifest


def test_the_restore_checks_pass_on_a_faithful_restore(tmp_path):
    lake, db, manifest = _restored(tmp_path)

    results = b.restore_checks(lake, db, manifest, sample_size=1,
                               rows_at=lambda uri: 12,
                               reparse=lambda uris: {"counts": {"IDENTICAL": len(uris)}})

    assert [result.status for result in results] == [b.PASS] * 3


def test_a_pointer_the_restored_cache_does_not_name_fails(tmp_path):
    lake, db, manifest = _restored(tmp_path)
    db.cache_run_id = "mp-20260101T0000Z"

    result = b.check_pointer_matches_cache(lake, db, manifest)

    assert result.status == b.FAIL and result.observed == {"pointer": RUN_ID, "cache": "mp-20260101T0000Z"}


def test_a_dataset_whose_row_count_moved_fails(tmp_path):
    lake, db, manifest = _restored(tmp_path)

    result = b.check_datasets_present(lake, manifest, rows_at=lambda uri: 11)

    assert result.status == b.FAIL
    assert result.observed["wrong"] == [{"dataset": "offer_current", "rows": 11, "manifest_rows": 12}]


def test_a_dataset_missing_its_success_marker_fails_rather_than_counting_zero(tmp_path):
    lake, db, manifest = _restored(tmp_path)

    result = b.check_datasets_present(lake, manifest, rows_at=lambda uri: None)

    assert result.status == b.FAIL


def test_a_diverged_reparse_fails_the_restore(tmp_path):
    _, db, _ = _restored(tmp_path)

    result = b.check_reparse_sample(db, sample_size=1,
                                    reparse=lambda uris: {"counts": {"DIVERGED": 1},
                                                          "problems": [{"raw_artifact_id": "raw-1"}]})

    assert result.status == b.FAIL


def test_a_restore_whose_audit_names_no_raw_artifact_fails_rather_than_passing_empty(tmp_path):
    _, db, _ = _restored(tmp_path)
    db.attempts = []

    result = b.check_reparse_sample(db, sample_size=3, reparse=lambda uris: {"counts": {}})

    # Nothing sampled is not "nothing wrong": the Bronze copy went unproven.
    assert result.status == b.FAIL


def test_a_check_that_raises_is_a_failure_carrying_its_error(tmp_path):
    lake, db, manifest = _restored(tmp_path)

    def explode(uri):
        raise RuntimeError("MinIO is down")

    results = b.restore_checks(lake, db, manifest, sample_size=1, rows_at=explode,
                               reparse=lambda uris: {"counts": {"IDENTICAL": len(uris)}})
    failed = [result for result in results if result.status == b.FAIL]

    assert len(failed) == 1 and "MinIO is down" in str(failed[0].observed)


def test_the_report_is_sorted_and_deterministic(tmp_path):
    lake, db, manifest = _restored(tmp_path)
    kwargs = dict(sample_size=1, rows_at=lambda uri: 12,
                  reparse=lambda uris: {"counts": {"IDENTICAL": len(uris)}})

    first = b.report(b.restore_checks(lake, db, manifest, **kwargs))

    assert first == b.report(b.restore_checks(lake, db, manifest, **kwargs))
    names = [check["check"] for check in json.loads(first)["checks"]]
    assert names == sorted(names)
    assert json.loads(first)["passed"] is True


# --- zone keys ---------------------------------------------------------------

def test_a_gold_uri_resolves_to_a_key_inside_its_zone():
    assert b.zone_key("s3a://ecommerce-gold/marketplace/run_id=x", "gold") == "marketplace/run_id=x"


def test_a_uri_from_another_profile_still_resolves_to_the_same_key():
    # A manifest written against MinIO, read back on a local-profile machine.
    assert b.zone_key("s3a://some-other-bucket/marketplace/run_id=x", "gold").endswith("marketplace/run_id=x")


# --- the CLI -----------------------------------------------------------------

def test_the_ops_cli_lists_backup_restore_and_backups():
    import subprocess
    import sys

    result = subprocess.run([sys.executable, "-m", "ops", "--help"], capture_output=True, text=True, timeout=120)

    assert result.returncode == 0
    for command in ("backup", "restore", "backups"):
        assert command in result.stdout


def test_restore_requires_the_backup_it_is_asked_for():
    import subprocess
    import sys

    result = subprocess.run([sys.executable, "-m", "ops", "restore"], capture_output=True, text=True, timeout=120)

    assert result.returncode != 0
    assert "--from" in result.stderr


# --- which attempts the reparse sample may be drawn from ---------------------

def _attempt(uri, *, error_kind=None, parsed=7):
    return (uri, error_kind, parsed)


def test_an_attempt_that_published_nothing_is_never_sampled(tmp_path):
    """Drill D2 leaves a raw body in Bronze whose observations never reached
    Silver, on purpose: Kafka was down, so ``parsed_count = acknowledged = 0``.
    Reparsing it reports NEW_OBSERVATIONS, correctly — and a restore check
    that sampled it would fail on a perfectly faithful copy."""
    _, db, _ = _restored(tmp_path)
    db.attempts = [_attempt("s3a://b/publish-error", error_kind="PUBLISH_ERROR", parsed=0),
                   _attempt("s3a://b/parse-error", error_kind="PARSE_ERROR", parsed=0),
                   _attempt("s3a://b/good-1"), _attempt("s3a://b/good-2")]
    sampled = []

    result = b.check_reparse_sample(db, sample_size=4,
                                    reparse=lambda uris: (sampled.extend(uris),
                                                          {"counts": {"IDENTICAL": len(uris)}})[1])

    assert sampled == ["s3a://b/good-1", "s3a://b/good-2"]
    assert result.status == b.PASS


def test_a_partially_published_attempt_is_never_sampled_either(tmp_path):
    """``parsed_count`` is the acknowledged count, so a partial publish leaves
    observations the reparse will find and Silver will not have. The audit
    cannot tell a partial publish from a complete one by count alone — but a
    partial one is always FAILED with an error_kind, and that it can."""
    _, db, _ = _restored(tmp_path)
    db.attempts = [_attempt("s3a://b/partial", error_kind="PUBLISH_ERROR", parsed=3),
                   _attempt("s3a://b/good")]
    sampled = []

    b.check_reparse_sample(db, sample_size=4,
                           reparse=lambda uris: (sampled.extend(uris),
                                                 {"counts": {"IDENTICAL": len(uris)}})[1])

    assert sampled == ["s3a://b/good"]


def test_an_audit_with_only_unpublished_attempts_fails_rather_than_sampling_none(tmp_path):
    _, db, _ = _restored(tmp_path)
    db.attempts = [_attempt("s3a://b/publish-error", error_kind="PUBLISH_ERROR", parsed=0)]

    result = b.check_reparse_sample(db, sample_size=4, reparse=lambda uris: {"counts": {}})

    assert result.status == b.FAIL


# --- validate on a stack that was just restored ------------------------------

def _check(name, status):
    return b.CheckResult(name, status, None, None)


def test_the_derived_stores_are_allowed_to_be_empty_after_a_restore():
    results = [_check("bronze_present", b.PASS),
               _check("es_changes_unique", b.FAIL),
               _check("kafka_to_silver_lag", b.FAIL),
               _check("redis_offer_state_present", b.FAIL)]

    ok, tolerated, unexpected = b.judge_restored_validate(results)

    # Plan 12.1 backs up none of Kafka, Elasticsearch or Redis, and 12.3 step
    # 4 says they refill from new crawls. A fresh restore has not crawled, and
    # its broker has never seen the Silver consumer group at all.
    assert ok is True
    assert tolerated == ["es_changes_unique", "kafka_to_silver_lag", "redis_offer_state_present"]
    assert unexpected == []


def test_any_other_failing_check_still_fails_a_restore():
    results = [_check("es_changes_unique", b.FAIL), _check("pointer_matches_cache", b.FAIL)]

    ok, tolerated, unexpected = b.judge_restored_validate(results)

    assert ok is False and unexpected == ["pointer_matches_cache"]


def test_a_fully_green_validate_passes_the_restored_judgement_too():
    ok, tolerated, unexpected = b.judge_restored_validate([_check("bronze_present", b.PASS)])

    assert (ok, tolerated, unexpected) == (True, [], [])


def test_the_tolerated_checks_are_exactly_the_stores_that_are_not_backed_up():
    # Pinned so widening the allowance is a deliberate edit with a reason.
    assert b.RESTORED_STACK_EXPECTED_EMPTY == ("es_changes_unique", "kafka_to_silver_lag",
                                               "redis_offer_state_present")


def test_the_mp_script_holds_no_stray_control_characters():
    """A guard, not a style rule.

    An edit that wrote ``\backups`` through a layer that read it as an escape
    put two backspace characters into mp.ps1. PowerShell parsed the file
    happily and then looked for a path that did not exist, so the failure
    surfaced minutes later as "the quality gate refused the restored Silver",
    which was not what had happened.
    """
    from pathlib import Path

    script = (Path(__file__).resolve().parents[1] / "scripts" / "mp.ps1").read_text(encoding="utf-8")
    stray = {character for character in script if ord(character) < 32 and character not in "\n\r\t"}

    assert stray == set(), f"mp.ps1 holds control characters: {[hex(ord(c)) for c in stray]}"

"""Phase 8 plan section 9.2: `ops validate` (test 23)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from ops import validate as v

AS_OF = datetime(2026, 10, 2, 7, tzinfo=timezone.utc)
LIMITS = v.Limits(reconciliation_lookback_seconds=172800, reconciliation_settle_seconds=900, offer_ttl_seconds=86400)
SILVER = "file:///lake/silver/marketplace/offer_observations"


def _silver_event(run_id, observed_at, observation_id):
    return json.dumps({"crawl_run_id": run_id, "payload": {"observation": {
        "observation_id": observation_id, "observed_at": observed_at.isoformat()}}}).encode()


class FakeSources:
    """A healthy stack. Each test breaks exactly one thing."""

    def __init__(self):
        from config.quality_rules import QUALITY_RULES

        self.raw = {"s3a://ecommerce-bronze/raw/a.bin": True}
        self.attempt_parsed = {"run-1": 2}
        self.run_completed = {"run-1": AS_OF - timedelta(hours=1)}
        day = (AS_OF - timedelta(hours=2)).date().isoformat()
        self.silver = {
            f"{SILVER}/marketplace=tiki/observed_date={day}/observation_id=o1.json": _silver_event("run-1", AS_OF - timedelta(hours=2), "o1"),
            f"{SILVER}/marketplace=tiki/observed_date={day}/observation_id=o2.json": _silver_event("run-1", AS_OF - timedelta(hours=2), "o2"),
        }
        self.lag = 0
        self.stages = ["DECODE", "CONTRACT_VALIDATION"]
        self.speed = [(7, "SUCCEEDED")]
        self.es = {
            "marketplace-changes-v1": [{"event_id": "e1"}, {"event_id": "e2"}],
            "marketplace-offers-current-v1": [{"offer_id": "offer-1", "observed_at": AS_OF.isoformat()}],
        }
        self.redis = {"rt:offer:offer-1"}
        self.batch = [("mp-20261002T0700Z", "SUCCEEDED", len(QUALITY_RULES))]
        self.pointer = {"run_id": "mp-20261002T0700Z", "as_of": AS_OF.isoformat(),
                        "datasets": [{"dataset_name": "offer_current", "uri": "s3a://ecommerce-gold/x/offer_current", "row_count": 2}]}
        self.cache = [("mp-20261002T0700Z",)]
        self.gold_rows = {"s3a://ecommerce-gold/x/offer_current": 2}
        self.quality = [(rule.check_name,) for rule in QUALITY_RULES]

    def query(self, sql, params=()):
        if "SELECT raw_uri" in sql:
            return [(uri,) for uri in self.raw]
        if "sum(parsed_count)" in sql:
            return list(self.attempt_parsed.items())
        if "FROM audit.crawl_run" in sql:
            return list(self.run_completed.items())
        if "marketplace_speed_batch" in sql:
            return self.speed
        if "FROM audit.marketplace_batch_run" in sql:
            return self.batch
        if "marketplace_cache_version" in sql:
            return self.cache
        if "FROM audit.marketplace_quality_result" in sql:
            return self.quality
        raise AssertionError(sql)

    def object_exists(self, uri): return self.raw.get(uri, False)
    def list_objects(self, prefix): return sorted(uri for uri in self.silver if uri.startswith(prefix))
    def read_object(self, uri): return self.silver.get(uri)
    def parquet_rows(self, uri): return self.gold_rows.get(uri)
    def consumer_lag(self, group, topic): return self.lag
    def dlq_stages(self): return list(self.stages)
    def es_documents(self, index): return list(self.es.get(index, []))
    def redis_exists(self, key): return key in self.redis
    def current_pointer(self): return self.pointer


@pytest.fixture(autouse=True)
def _local_lake(monkeypatch):
    import config.settings as settings

    monkeypatch.setattr(settings, "data_lake_uri", lambda zone, dataset="": f"file:///lake/{zone}/{dataset}")


def by_name(results):
    return {result.check: result for result in results}


def test_a_healthy_stack_passes_every_check():
    results = v.validate(FakeSources(), LIMITS)

    assert [r.status for r in results] == [v.PASS] * len(v.CHECKS), [r for r in results if r.status != v.PASS]


BREAKS = {
    "bronze_present": lambda s: s.raw.update({"s3a://ecommerce-bronze/raw/a.bin": False}),
    "kafka_to_silver_lag": lambda s: setattr(s, "lag", 3),
    "silver_reconciles_with_audit": lambda s: s.attempt_parsed.update({"run-1": 3}),
    "dlq_only_bad_records": lambda s: s.stages.append("SILVER_WRITE"),
    "speed_last_batch_succeeded": lambda s: setattr(s, "speed", [(8, "FAILED")]),
    "es_changes_unique": lambda s: s.es["marketplace-changes-v1"].append({"event_id": "e1"}),
    "redis_offer_state_present": lambda s: s.redis.clear(),
    "batch_latest_terminal": lambda s: setattr(s, "batch", [("mp-20261002T0700Z", "FAILED", 0)]),
    "pointer_matches_cache": lambda s: setattr(s, "cache", [("mp-20261001T0000Z",)]),
    "pointer_gold_exists": lambda s: s.gold_rows.update({"s3a://ecommerce-gold/x/offer_current": 1}),
    "quality_results_complete": lambda s: s.quality.pop(),
}


def test_every_check_has_a_failing_case():
    assert sorted(BREAKS) == sorted(check.__name__ for check in v.CHECKS)


@pytest.mark.parametrize("name", sorted(BREAKS))
def test_each_check_fails_on_its_own_defect_and_only_it(name):
    sources = FakeSources()
    BREAKS[name](sources)

    results = by_name(v.validate(sources, LIMITS))

    assert results[name].status == v.FAIL
    assert [check for check, result in results.items() if result.status == v.FAIL] == [name]


def test_a_quality_failed_run_with_stored_results_is_terminal():
    sources = FakeSources()
    sources.batch = [("mp-20261002T0700Z", "QUALITY_FAILED", 13)]

    assert by_name(v.validate(sources, LIMITS))["batch_latest_terminal"].status == v.PASS


def test_reconciliation_ignores_runs_outside_the_settled_window():
    sources = FakeSources()
    # Still settling: finished inside the settle delay before as_of.
    sources.run_completed["run-1"] = AS_OF - timedelta(seconds=60)
    sources.attempt_parsed["run-1"] = 99

    assert by_name(v.validate(sources, LIMITS))["silver_reconciles_with_audit"].status == v.PASS


def test_silver_rows_without_audit_are_a_mismatch():
    sources = FakeSources()
    day = (AS_OF - timedelta(hours=3)).date().isoformat()
    sources.silver[f"{SILVER}/marketplace=tiki/observed_date={day}/observation_id=o9.json"] = _silver_event(
        "run-unknown", AS_OF - timedelta(hours=3), "o9")

    result = by_name(v.validate(sources, LIMITS))["silver_reconciles_with_audit"]

    assert result.status == v.FAIL
    assert result.observed["mismatched"][0]["crawl_run_id"] == "run-unknown"


def test_an_expired_offer_is_not_expected_in_redis():
    sources = FakeSources()
    sources.es["marketplace-offers-current-v1"].append(
        {"offer_id": "offer-old", "observed_at": (AS_OF - timedelta(days=3)).isoformat()})

    assert by_name(v.validate(sources, LIMITS))["redis_offer_state_present"].status == v.PASS


def test_a_check_that_raises_fails_with_its_error():
    sources = FakeSources()
    sources.consumer_lag = lambda group, topic: (_ for _ in ()).throw(ConnectionError("broker down"))

    result = by_name(v.validate(sources, LIMITS))["kafka_to_silver_lag"]

    assert result.status == v.FAIL
    assert "broker down" in result.observed


def test_the_report_is_sorted_and_deterministic():
    first = v.report(v.validate(FakeSources(), LIMITS))
    second = v.report(v.validate(FakeSources(), LIMITS, checks=reversed(v.CHECKS)))

    assert first == second
    names = [row["check"] for row in json.loads(first)["checks"]]
    assert names == sorted(names)
    assert json.loads(first)["passed"] is True


# ---------------------------------------------------------------------------
# The smoke's waits (plan 9.1) and the CLI.
# ---------------------------------------------------------------------------
from ops import smoke  # noqa: E402

SINCE = AS_OF - timedelta(minutes=10)


class SmokeSources(FakeSources):
    def __init__(self, successes=2, changes=1):
        super().__init__()
        self.successes, self.changes = successes, changes

    def query(self, sql, params=()):
        if "SELECT f.target, count(*)" in sql:
            return [(task, self.successes) for task in params[0]]
        if "sum(a.parsed_count)" in sql:
            return [("run-1", 2)]
        if "change_rows > 0" in sql:
            return [(self.changes,)]
        return super().query(sql, params)


def test_the_smoke_targets_are_the_ones_seed_creates():
    from crawler.scheduling import CrawlTier
    from crawler.seed_frontier import seed

    class Recording:
        def __init__(self): self.targets = []
        def enqueue(self, task): self.targets.append(task.target); return True

    frontier = Recording()
    seed(frontier, marketplace_code="tiki", marketplace_id="marketplace-tiki", categories=smoke.SMOKE_CATEGORIES,
         pages=smoke.SMOKE_PAGES, tier=CrawlTier.ACTIVE, priority=0, max_attempts=5)

    assert frontier.targets == smoke.smoke_targets()
    assert len(set(frontier.targets)) == len(smoke.SMOKE_CATEGORIES) * smoke.SMOKE_PAGES


def test_the_smoke_waits_until_every_condition_holds():
    lines = []

    assert smoke.wait(SmokeSources(), targets=["t1", "t2"], timeout_seconds=60, since=SINCE,
                      log=lines.append, sleep=lambda s: None)
    assert json.loads(lines[-1])["conditions"]["silver_holds_parsed_observations"]["observed"] == {"expected": 2, "found": 2}


@pytest.mark.parametrize("overrides", [{"successes": 1}, {"changes": 0}])
def test_the_smoke_times_out_while_a_condition_is_unmet(overrides):
    clock = iter(range(0, 1000, 30))

    assert not smoke.wait(SmokeSources(**overrides), targets=["t1"], timeout_seconds=60, since=SINCE,
                          log=lambda line: None, sleep=lambda s: None, monotonic=lambda: next(clock))


def test_the_ops_cli_answers_help():
    import subprocess
    import sys

    result = subprocess.run([sys.executable, "-m", "ops", "--help"], capture_output=True, text=True, timeout=300)

    assert result.returncode == 0, result.stderr
    for command in ("migrate", "seed-smoke", "smoke-wait", "batch-plan", "validate", "status"):
        assert command in result.stdout


def test_the_batch_plan_names_a_minute_aligned_scheduled_run(capsys):
    from ops.__main__ import batch_plan

    batch_plan(60)
    plan = json.loads(capsys.readouterr().out)

    assert plan["run_id"].startswith("mp-")
    assert plan["as_of"].endswith(":00Z")


def test_the_quiet_wait_returns_once_the_sink_has_caught_up():
    sources = FakeSources()
    lags = iter([3, 1, 0])
    sources.consumer_lag = lambda group, topic: next(lags)

    assert smoke.wait_for_quiet(sources, timeout_seconds=60, log=lambda line: None, sleep=lambda s: None)


def test_the_quiet_wait_gives_up_after_its_timeout():
    sources = FakeSources()
    sources.consumer_lag = lambda group, topic: 5
    clock = iter(range(0, 1000, 30))

    assert not smoke.wait_for_quiet(sources, timeout_seconds=60, log=lambda line: None,
                                    sleep=lambda s: None, monotonic=lambda: next(clock))


def test_the_smoke_batch_window_covers_every_settled_crawl_run(monkeypatch, capsys):
    import ops.__main__ as cli
    import ops.validate as validate_module

    last = datetime(2026, 10, 2, 9, 37, 50, tzinfo=timezone.utc)

    class Last:
        def query(self, sql, params=()): return [(last,)]

    monkeypatch.setattr(validate_module, "LiveSources", Last)
    ticks = iter([datetime(2026, 10, 2, 9, 38, 10, tzinfo=timezone.utc), datetime(2026, 10, 2, 9, 39, 5, tzinfo=timezone.utc),
                  datetime(2026, 10, 2, 9, 39, 5, tzinfo=timezone.utc)])
    slept = []

    cli.batch_plan(60, after_last_crawl=True, sleep=slept.append, now=lambda: next(ticks))
    plan = json.loads(capsys.readouterr().out)

    # 09:37:50 + 60 s = 09:38:50, up to the next minute.
    assert plan == {"run_id": "mp-20261002T0939Z", "as_of": "2026-10-02T09:39:00Z"}
    assert slept, "it waited for the clock to pass as_of"


# The smoke's made-up categories must never outlive the smoke: a later `mp up`
# with the real TIKI_LISTING_URL would crawl the live marketplace for them.
class RecordingConnection:
    def __init__(self, log): self.log = log
    def __enter__(self): return self
    def __exit__(self, *exc): return False
    def cursor(self): return self
    def execute(self, sql, params=()): self.log.append((" ".join(sql.split()), params))
    @property
    def rowcount(self): return 6


def test_parking_disables_every_pending_smoke_task_and_only_those():
    log = []

    parked = smoke.park(lambda: RecordingConnection(log))

    (sql, params), = log
    assert sql.startswith("UPDATE audit.crawl_frontier SET status = 'DISABLED'")
    assert "status IN ('READY', 'RETRY_WAIT', 'LEASED')" in sql
    assert "marketplace_code = 'tiki'" in sql
    assert params[0] == smoke.smoke_targets()
    assert parked == 6


def test_activating_reenables_parked_smoke_tasks_now():
    log = []

    smoke.activate(lambda: RecordingConnection(log))

    (sql, params), = log
    assert sql.startswith("UPDATE audit.crawl_frontier SET status = 'READY'")
    assert "scheduled_for = now()" in sql
    assert "status = 'DISABLED'" in sql
    assert params[0] == smoke.smoke_targets()


def test_a_topic_without_partitions_is_an_error_not_zero_lag(monkeypatch):
    import kafka

    class NoTopic:
        def __init__(self, **kwargs): pass
        def partitions_for_topic(self, topic): return None
        def list_consumer_group_offsets(self, group): return {}
        def close(self): pass

    monkeypatch.setattr(kafka, "KafkaConsumer", NoTopic)
    monkeypatch.setattr(kafka, "KafkaAdminClient", NoTopic)
    sources = v.LiveSources.__new__(v.LiveSources)

    with pytest.raises(RuntimeError, match="no partitions"):
        sources.consumer_lag("group", "marketplace.observations.v1")


# ---------------------------------------------------------------------------
# The drill harness (plan 10), offline: the drills themselves need the stack.
# ---------------------------------------------------------------------------
def test_wait_until_returns_the_observation_or_fails_with_it(monkeypatch):
    from ops import drills

    monkeypatch.setattr(drills.time, "sleep", lambda seconds: None)
    values = iter([1, 2, 3])
    assert drills.wait_until(lambda: ((n := next(values)) == 3, n), timeout=60, what="three") == 3

    clock = iter(range(0, 1000, 30))
    monkeypatch.setattr(drills.time, "monotonic", lambda: next(clock))
    with pytest.raises(drills.DrillFailed, match="waiting for never; last observed 7"):
        drills.wait_until(lambda: (False, 7), timeout=60, what="never")


def test_a_drill_record_keeps_every_step_with_its_time(tmp_path, monkeypatch):
    from ops import drills

    monkeypatch.setattr(drills, "RECORDS", tmp_path)
    record = drills.Record("d9")
    record.step("inject", stopped="kafka")
    record.step("verify", ok=True)
    record.passed = True

    written = json.loads(record.write().read_text(encoding="utf-8"))

    assert written["drill"] == "d9" and written["passed"] is True
    assert [step["step"] for step in written["steps"]] == ["inject", "verify"]
    assert all("at" in step for step in written["steps"])


def test_the_drills_cover_d1_to_d6():
    from ops import drills

    assert sorted(drills.DRILLS) == ["d1", "d2", "d3", "d4", "d5", "d6"]

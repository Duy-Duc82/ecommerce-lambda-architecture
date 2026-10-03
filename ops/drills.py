"""Failure drills against the live stack — Phase 8 plan section 10 (P1-12).

Every drill has the same shape:

    baseline  validate passes
    inject    one fault, through Compose (stop, kill, start) or the stub's mode
    observe   the system's state while faulted, asserted
    recover   remove the fault; no manual data repair
    verify    validate passes again, plus the drill's own invariant

A drill writes a JSON record of those steps, with timestamps, to
``data/ops/drills/<name>.json``: Phase 9's evidence input. Whatever happens,
it restores the stack to a passing ``validate`` before it returns, so
``drill all`` cannot cascade, and a drill that cannot reach its baseline fails.

Drills run on the host, not in the ``ops`` container: they need Docker. They
reach the stack through its published ports, so ``DATA_LAKE_PROFILE=minio``
and the host-side endpoints in ``.env`` must be in effect (``mp drill`` sets
the profile). The crawl universe is the smoke's (``ops.smoke``): drills
re-enable those made-up targets, point the crawler at the stub and park the
targets again when done. Nothing here contacts the live marketplace.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
RECORDS = ROOT / "data" / "ops" / "drills"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker-compose.yml")]
STUB_URL = "http://stub-source:8000/api/personalish/v1/blocks/listings"
# The crawler under drill: the stub, a one-minute cadence, and short retry,
# circuit and lease timings so a drill observes them in minutes, not hours.
CRAWL_ENV = {
    "TIKI_LISTING_URL": STUB_URL,
    "CRAWL_ACTIVE_CADENCE_MINUTES": "1",
    "CRAWL_RETRY_BASE_SECONDS": "2",
    "CRAWL_RETRY_MAX_SECONDS": "10",
    "CRAWL_CIRCUIT_OPEN_SECONDS": "30",
    "CRAWL_LEASE_SECONDS": "30",
    "CRAWL_SERVICE_WORKER_ID": "drill-a",
}
BATCH_SETTLE_SECONDS = 60
SERVICES = ("stub-source", "silver-sink", "speed", "batch-scheduler")


class DrillFailed(AssertionError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Record:
    name: str
    steps: list[dict] = field(default_factory=list)
    passed: bool = False
    error: str | None = None

    def step(self, kind: str, **detail: Any) -> None:
        self.steps.append({"step": kind, "at": _now(), **detail})
        print(json.dumps({"drill": self.name, "step": kind, **detail}, default=str), flush=True)

    def write(self) -> Path:
        RECORDS.mkdir(parents=True, exist_ok=True)
        path = RECORDS / f"{self.name}.json"
        path.write_text(json.dumps({"drill": self.name, "passed": self.passed, "error": self.error, "steps": self.steps},
                                   indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        return path


def wait_until(check: Callable[[], tuple[bool, Any]], *, timeout: float, what: str, poll: float = 5) -> Any:
    """Poll ``check`` until it reports success; return its observation, or raise."""
    deadline = time.monotonic() + timeout
    observed = None
    while True:
        ok, observed = check()
        if ok:
            return observed
        if time.monotonic() >= deadline:
            raise DrillFailed(f"timed out after {timeout:.0f}s waiting for {what}; last observed {observed}")
        time.sleep(poll)


class Stack:
    """Compose, the stub's mode endpoint, and read-only queries; nothing else."""

    def __init__(self) -> None:
        from ops.validate import LiveSources

        self.src = LiveSources()

    # -- Docker --
    @staticmethod
    def run(args: list[str], *, env: dict | None = None, check: bool = True) -> subprocess.CompletedProcess:
        import os

        result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True,
                                env={**os.environ, **(env or {})})
        if check and result.returncode != 0:
            raise DrillFailed(f"{' '.join(args)} failed ({result.returncode}): {result.stderr.strip()[-500:]}")
        return result

    def up(self, *services: str, env: dict | None = None) -> None:
        self.run(COMPOSE + ["--profile", "*", "up", "-d", "--no-deps", *services], env=env)

    def stop(self, *services: str) -> None:
        self.run(COMPOSE + ["--profile", "*", "stop", *services])

    def kill(self, container: str) -> None:
        self.run(["docker", "kill", container])

    def status(self, container: str) -> str:
        result = self.run(["docker", "inspect", "-f", "{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{end}}", container],
                          check=False)
        return result.stdout.strip() if result.returncode == 0 else "absent"

    def wait_healthy(self, container: str, timeout: float = 180) -> None:
        wait_until(lambda: (self.status(container).endswith("/healthy"), self.status(container)),
                   timeout=timeout, what=f"{container} healthy", poll=3)

    def stub_mode(self, mode: str) -> None:
        code = ("import urllib.request,sys; urllib.request.urlopen(urllib.request.Request("
                "'http://localhost:8000/_stub/mode', data=sys.argv[1].encode()), timeout=10)")
        self.run(["docker", "exec", "stub-source", "python", "-c", code, mode])

    # -- PostgreSQL and the lake --
    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        return self.src.query(sql, params)

    def db_now(self) -> datetime:
        (now,), = self.query("SELECT now()")
        return now

    def activate(self) -> int:
        from common.postgres import postgres_connection_factory
        from ops.smoke import activate

        return activate(postgres_connection_factory())

    def park(self) -> int:
        from common.postgres import postgres_connection_factory
        from ops.smoke import park

        return park(postgres_connection_factory())

    def start_crawler(self, **overrides: str) -> None:
        self.activate()
        self.up("crawl-worker", env={**CRAWL_ENV, **overrides})

    def stop_crawler(self) -> None:
        self.stop("crawl-worker")
        self.park()

    def attempts_since(self, since: datetime) -> list[tuple]:
        """(task_id, status, error_kind, parsed_count, raw_uri, started_at, completed_at, target) of smoke attempts."""
        from ops.smoke import smoke_targets

        return self.query(
            "SELECT a.task_id, a.status, a.error_kind, a.parsed_count, a.raw_uri, a.started_at, a.completed_at, f.target "
            "FROM audit.crawl_request_attempt a JOIN audit.crawl_frontier f USING (task_id) "
            "WHERE f.marketplace_code = 'tiki' AND f.target = ANY(%s) AND a.started_at >= %s ORDER BY a.started_at",
            (smoke_targets(), since))

    def every_target_succeeded_since(self, since: datetime) -> tuple[bool, Any]:
        from ops.smoke import smoke_targets

        done = {row[7] for row in self.attempts_since(since) if row[1] in ("SUCCEEDED", "PARTIAL")}
        return set(smoke_targets()) <= done, sorted(done)

    def quiet(self, timeout: float = 240) -> None:
        from ops.smoke import wait_for_quiet

        if not wait_for_quiet(self.src, timeout_seconds=timeout, log=lambda line: None):
            raise DrillFailed("the Silver sink did not catch up")

    def speed_batches_since(self, since: datetime) -> list[tuple]:
        return self.query("SELECT query_id, batch_id, status, input_rows, change_rows, started_at "
                          "FROM audit.marketplace_speed_batch WHERE started_at >= %s ORDER BY started_at", (since,))

    def batch_after_last_crawl(self) -> tuple[int, str]:
        """One batch over everything crawled so far, as the smoke runs it."""
        plan = self.run(COMPOSE + ["--profile", "ops", "run", "--rm", "--no-deps", "ops", "python", "-m", "ops",
                                   "batch-plan", "--after-last-crawl"],
                        env={"MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS": str(BATCH_SETTLE_SECONDS)})
        plan = json.loads(plan.stdout.strip().splitlines()[-1])
        result = self.run(COMPOSE + ["--profile", "batch", "run", "--rm", "--no-deps", "batch-once", "python3", "-m",
                                     "batch_layer.marketplace_warehouse", "--run-id", plan["run_id"], "--as-of", plan["as_of"]],
                          env={"MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS": str(BATCH_SETTLE_SECONDS)}, check=False)
        return result.returncode, plan["run_id"]

    def validate(self) -> list:
        from ops.validate import default_limits, validate

        return validate(self.src, default_limits())

    def es_change_ids(self) -> list[str]:
        from config.settings import ES_INDEX_MARKETPLACE_CHANGES

        return [doc["event_id"] for doc in self.src.es_documents(ES_INDEX_MARKETPLACE_CHANGES)]


def _failing(results) -> list[str]:
    return [r.check for r in results if r.status != "PASS"]


def baseline(stack: Stack, rec: Record) -> None:
    failing = _failing(stack.validate())
    rec.step("baseline", failing=failing)
    if failing:
        raise DrillFailed(f"baseline validate fails: {failing}; a drill never starts from a broken stack")


def restore(stack: Stack, rec: Record) -> list[str]:
    """Back to a passing validate, whatever the drill left behind."""
    stack.run(COMPOSE + ["up", "-d"])                 # any core service a drill stopped
    stack.up(*SERVICES)                                # and the marketplace services
    stack.wait_healthy("stub-source")
    stack.stub_mode("ok")
    stack.stop_crawler()
    stack.quiet()
    # The speed query may still be finishing the last micro-batch.
    failing = wait_until(lambda: (not _failing(stack.validate()), _failing(stack.validate())),
                         timeout=240, what="validate to pass after recovery", poll=15)
    rec.step("restored", failing=failing or [])
    return failing or []


# ---------------------------------------------------------------------------
# D1-D6
# ---------------------------------------------------------------------------

def d1_source_errors(stack: Stack, rec: Record) -> None:
    """429, then 500, then a timeout: classified, retried, circuit opened, then recovered."""
    from config.settings import CRAWL_CIRCUIT_FAILURE_THRESHOLD

    since = stack.db_now()
    expected = {"429": "RATE_LIMITED", "500": "SERVER_ERROR", "timeout": "TRANSIENT_NETWORK"}
    stack.stub_mode("429")
    stack.start_crawler()
    for mode, kind in expected.items():
        stack.stub_mode(mode)
        rec.step("inject", stub_mode=mode)
        mode_since = stack.db_now()
        rows = wait_until(lambda: ((r := [a for a in stack.attempts_since(mode_since) if a[2] == kind]) != [], r),
                          timeout=240, what=f"an attempt classified {kind}")
        rec.step("observe", stub_mode=mode, error_kind=kind, attempts=len(rows))
        if kind == "RATE_LIMITED":
            # Retry-After: 1 s. The task is not due again before the header allows.
            task_id, completed = rows[0][0], rows[0][6]
            (scheduled,), = stack.query("SELECT scheduled_for FROM audit.crawl_frontier WHERE task_id = %s", (task_id,))
            if scheduled < completed + timedelta(seconds=1):
                raise DrillFailed(f"Retry-After ignored: due {scheduled} after a 429 at {completed}")
            rec.step("observe", retry_after_honoured=True, scheduled_for=scheduled, completed_at=completed)
    state = wait_until(
        lambda: ((row := stack.query("SELECT consecutive_failures, opened_until FROM audit.crawl_source_state "
                                     "WHERE marketplace_code = 'tiki'")) and row[0][1] is not None, row),
        timeout=180, what="the circuit to open")
    rec.step("observe", circuit=state, threshold=CRAWL_CIRCUIT_FAILURE_THRESHOLD)
    stack.stub_mode("ok")
    recovered_from = stack.db_now()
    rec.step("recover", stub_mode="ok")
    wait_until(lambda: stack.every_target_succeeded_since(recovered_from), timeout=300,
               what="every target to succeed once the source recovers")
    # No task gave up while it still had attempts left.
    gave_up = stack.query("SELECT task_id, attempts, max_attempts FROM audit.crawl_frontier "
                          "WHERE status = 'FAILED' AND attempts < max_attempts AND updated_at >= %s", (since,))
    stack.stop_crawler()
    rec.step("verify", failed_with_attempts_left=gave_up)
    if gave_up:
        raise DrillFailed(f"tasks FAILED with attempts left: {gave_up}")


def d2_kafka_down_during_crawl(stack: Stack, rec: Record) -> None:
    """Kafka stops mid-cycle: PUBLISH_ERROR, raw kept; afterwards check 8 still reconciles."""
    since = stack.db_now()
    stack.start_crawler()
    wait_until(lambda: (bool(stack.attempts_since(since)), None), timeout=120, what="the crawl to start")
    stack.stop("kafka")
    rec.step("inject", stopped="kafka")
    rows = wait_until(lambda: ((r := [a for a in stack.attempts_since(since) if a[2] == "PUBLISH_ERROR"]) != [], r),
                      timeout=300, what="a PUBLISH_ERROR attempt")
    for task_id, status, kind, parsed, raw_uri, *_ in rows:
        if raw_uri is None or not stack.src.object_exists(raw_uri):
            raise DrillFailed(f"PUBLISH_ERROR attempt of {task_id} lost its raw body: {raw_uri}")
        if not 0 <= parsed <= 2:
            raise DrillFailed(f"parsed_count {parsed} cannot be an acknowledged count for a 2-observation page")
    rec.step("observe", publish_errors=len(rows), parsed_counts=sorted(r[3] for r in rows), raw_kept=True)
    stack.run(COMPOSE + ["up", "-d", "kafka"])
    stack.wait_healthy("kafka")
    recovered_from = stack.db_now()
    rec.step("recover", started="kafka")
    wait_until(lambda: stack.every_target_succeeded_since(recovered_from), timeout=300,
               what="every target to succeed after Kafka returns")
    stack.stop_crawler()
    stack.quiet()
    code, run_id = stack.batch_after_last_crawl()
    (status,), = stack.query("SELECT status FROM audit.marketplace_batch_run WHERE run_id = %s", (run_id,))
    rec.step("verify", batch=run_id, exit_code=code, status=status)
    if code != 0 or status != "SUCCEEDED":
        raise DrillFailed(f"batch {run_id} after partial publishes: exit {code}, {status} (check 8 must reconcile)")


def d3_minio_down_during_sink(stack: Stack, rec: Record) -> None:
    """MinIO stops while the sink has work: no commit, lag grows, no DLQ; then it catches up."""
    from config.settings import KAFKA_SILVER_CONSUMER_GROUP
    from config.topics import MARKETPLACE_OBSERVATIONS
    from ops.smoke import silver_condition, smoke_targets

    lag_of = lambda: stack.src.consumer_lag(KAFKA_SILVER_CONSUMER_GROUP, MARKETPLACE_OBSERVATIONS.name)  # noqa: E731
    dlq_before = len(stack.src.dlq_stages())
    stack.stop("silver-sink")
    since = stack.db_now()
    stack.start_crawler()
    wait_until(lambda: stack.every_target_succeeded_since(since), timeout=300, what="one crawl of every target")
    stack.stop_crawler()
    stack.stop("minio")
    rec.step("inject", stopped="minio")
    stack.up("silver-sink")
    lag = wait_until(lambda: ((n := lag_of()) > 0, n), timeout=60, what="a backlog for the sink")
    time.sleep(60)
    lag_later = lag_of()
    dlq_faulted = len(stack.src.dlq_stages())
    rec.step("observe", lag=lag, lag_after_60s=lag_later, dlq_before=dlq_before, dlq_while_faulted=dlq_faulted)
    if lag_later < lag:
        raise DrillFailed(f"the sink committed while MinIO was down: lag {lag} -> {lag_later}")
    if dlq_faulted != dlq_before:
        raise DrillFailed(f"a storage outage reached the DLQ: {dlq_before} -> {dlq_faulted}")
    stack.run(COMPOSE + ["up", "-d", "minio"])
    stack.wait_healthy("minio")
    rec.step("recover", started="minio")
    stack.quiet()
    silver = silver_condition(stack.src, targets=smoke_targets(), since=since)
    dlq_after = len(stack.src.dlq_stages())
    rec.step("verify", silver=silver.observed, dlq_after=dlq_after)
    if not silver.met:
        raise DrillFailed(f"Silver does not hold every acknowledged observation: {silver.observed}")
    if dlq_after != dlq_before:
        raise DrillFailed(f"DLQ changed: {dlq_before} -> {dlq_after}")


def _crawl_once(stack: Stack) -> datetime:
    since = stack.db_now()
    stack.start_crawler()
    wait_until(lambda: stack.every_target_succeeded_since(since), timeout=300, what="one crawl of every target")
    stack.stop_crawler()
    return since


def d4_sinks_down_during_speed(stack: Stack, rec: Record) -> None:
    """ES, then Redis, down under the speed query: the batch fails, then succeeds on recovery."""
    for service in ("elasticsearch", "redis"):
        stack.stop(service)
        faulted = stack.db_now()
        rec.step("inject", stopped=service)
        _crawl_once(stack)
        failed = wait_until(lambda: ((b := [r for r in stack.speed_batches_since(faulted) if r[2] == "FAILED"]) != [], b),
                            timeout=300, what=f"a FAILED speed batch with {service} down")
        rec.step("observe", service=service, failed_batches=[(r[1], r[3]) for r in failed])
        stack.run(COMPOSE + ["up", "-d", service])
        stack.wait_healthy(service)
        recovered = stack.db_now()
        rec.step("recover", started=service)
        ok = wait_until(lambda: ((b := [r for r in stack.speed_batches_since(recovered) if r[2] == "SUCCEEDED" and r[3] > 0]) != [], b),
                        timeout=360, what=f"the failed batch to succeed after {service} returns")
        ids = stack.es_change_ids()
        rec.step("verify", service=service, succeeded_batches=[(r[1], r[3]) for r in ok],
                 es_changes=len(ids), distinct=len(set(ids)))
        if len(ids) != len(set(ids)):
            raise DrillFailed("duplicate change documents in Elasticsearch")


def d5_speed_restart(stack: Stack, rec: Record) -> None:
    """Kill the query mid-stream; then replay everything from a fresh checkpoint."""
    since = stack.db_now()
    stack.start_crawler()
    wait_until(lambda: (any(r[3] > 0 for r in stack.speed_batches_since(since)), None),
               timeout=300, what="the speed query to be processing")
    stack.kill("speed")
    rec.step("inject", killed="speed")
    stack.up("speed")
    restarted = stack.db_now()
    rec.step("recover", started="speed")
    wait_until(lambda: stack.every_target_succeeded_since(restarted), timeout=300, what="more crawls after the restart")
    stack.stop_crawler()
    stack.quiet()
    wait_until(lambda: (any(r[2] == "SUCCEEDED" for r in stack.speed_batches_since(restarted)), None),
               timeout=240, what="a speed batch after the restart")
    time.sleep(60)   # the trigger is 30 s: let the last observations through
    ids = stack.es_change_ids()
    rec.step("verify", variant="kill", es_changes=len(ids), distinct=len(set(ids)))
    if len(ids) != len(set(ids)):
        raise DrillFailed("the restart duplicated change documents")

    # Variant 2: no checkpoint at all. A full replay from earliest must yield
    # the same change documents, since every _id is deterministic.
    before = set(ids)
    stack.run(COMPOSE + ["--profile", "speed", "rm", "-sf", "speed"])
    volume = stack.run(["docker", "volume", "ls", "-q", "--filter", "name=speed_checkpoints"]).stdout.split()
    for name in volume:
        stack.run(["docker", "volume", "rm", name])
    replay_from = stack.db_now()
    stack.up("speed", env={"MARKETPLACE_STREAM_CHECKPOINT_VERSION": "drill-d5"})
    rec.step("inject", variant="replay", removed_volumes=volume, checkpoint_version="drill-d5")
    wait_until(lambda: (any(r[2] == "SUCCEEDED" and r[3] > 0 for r in stack.speed_batches_since(replay_from)), None),
               timeout=300, what="the replay's first batch")
    def settled() -> tuple[bool, Any]:
        recent = [r for r in stack.speed_batches_since(replay_from) if r[3] > 0]
        return bool(recent) and (datetime.now(timezone.utc) - recent[-1][5]).total_seconds() > 60, len(recent)
    wait_until(settled, timeout=600, what="the replay to drain", poll=15)
    after = set(stack.es_change_ids())
    rec.step("verify", variant="replay", before=len(before), after=len(after),
             new=len(after - before), missing=len(before - after))
    if after != before:
        raise DrillFailed(f"replay changed the change set: {len(after - before)} new, {len(before - after)} missing")
    # Leave the default checkpoint version in place for the next drill.
    stack.up("speed")


def d6_expired_lease(stack: Stack, rec: Record) -> None:
    """Kill a worker holding leases; a second worker recovers them only after they expire."""
    from ops.smoke import smoke_targets

    stack.stub_mode("timeout")
    stack.start_crawler(CRAWL_SERVICE_WORKER_ID="drill-a")
    held = ("SELECT task_id, lease_expires_at FROM audit.crawl_frontier WHERE status = 'LEASED' "
            "AND lease_owner = 'drill-a' AND target = ANY(%s)")
    wait_until(lambda: (bool(stack.query(held, (smoke_targets(),))), None), timeout=120,
               what="worker A to hold leases", poll=1)
    stack.kill("crawl-worker")
    killed_at = stack.db_now()
    # Read after the kill: A may have released one task (a timed-out fetch)
    # between the first sight of its leases and its death.
    leased = stack.query(held, (smoke_targets(),))
    if not leased:
        raise DrillFailed("worker A held no lease when it was killed")
    rec.step("inject", killed="crawl-worker (drill-a)", leases=len(leased))
    stack.stub_mode("ok")
    stack.up("crawl-worker", env={**CRAWL_ENV, "CRAWL_SERVICE_WORKER_ID": "drill-b"})
    rec.step("recover", started="crawl-worker (drill-b)")
    earliest_expiry = min(expiry for _, expiry in leased)
    if stack.db_now() < earliest_expiry:
        still = stack.query("SELECT count(*) FROM audit.crawl_frontier WHERE task_id = ANY(%s) AND status = 'LEASED' "
                            "AND lease_owner = 'drill-a'", ([t for t, _ in leased],))[0][0]
        rec.step("observe", before_expiry=True, still_leased_by_a=still, of=len(leased))
        if still != len(leased):
            raise DrillFailed(f"worker B took {len(leased) - still} lease(s) before they expired")
    wait_until(lambda: stack.every_target_succeeded_since(killed_at), timeout=300,
               what="worker B to complete the recovered tasks")
    stack.stop_crawler()
    attempts = stack.attempts_since(killed_at - timedelta(minutes=5))
    by_task: dict[str, list[tuple]] = {}
    for row in attempts:
        by_task.setdefault(row[0], []).append((row[5], row[6]))
    overlaps = [task for task, spans in by_task.items()
                if any(a_end > b_start for (a_start, a_end), (b_start, _) in zip(sorted(spans), sorted(spans)[1:]))]
    recovered = [t for t, _ in leased if any(row[0] == t and row[1] in ("SUCCEEDED", "PARTIAL") for row in attempts)]
    rec.step("verify", recovered=len(recovered), of=len(leased), concurrent_attempts=overlaps)
    if overlaps:
        raise DrillFailed(f"tasks processed twice at once: {overlaps}")
    if len(recovered) != len(leased):
        raise DrillFailed(f"only {len(recovered)} of {len(leased)} expired leases were recovered")


DRILLS: dict[str, Callable[[Stack, Record], None]] = {
    "d1": d1_source_errors,
    "d2": d2_kafka_down_during_crawl,
    "d3": d3_minio_down_during_sink,
    "d4": d4_sinks_down_during_speed,
    "d5": d5_speed_restart,
    "d6": d6_expired_lease,
}


def run(name: str, stack: Stack | None = None) -> Record:
    stack = stack or Stack()
    rec = Record(name)
    try:
        baseline(stack, rec)
        DRILLS[name](stack, rec)
        rec.passed = True
    except Exception as error:  # noqa: BLE001 - recorded, then the stack is restored
        rec.error = f"{type(error).__name__}: {error}"[:2000]
        rec.step("failed", error=rec.error)
    finally:
        try:
            failing = restore(stack, rec)
        except Exception as error:  # noqa: BLE001
            failing = [f"restore: {type(error).__name__}: {error}"[:500]]
            rec.step("restore_failed", error=failing[0])
        if failing:
            rec.passed = False
            rec.error = rec.error or f"stack not restored: {failing}"
        rec.write()
    return rec


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Phase 8 failure drills against the live stack")
    parser.add_argument("drill", choices=[*DRILLS, "all"])
    args = parser.parse_args(argv)
    names = list(DRILLS) if args.drill == "all" else [args.drill]
    results = {name: run(name).passed for name in names}
    print(json.dumps({"drills": results}, sort_keys=True))
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())

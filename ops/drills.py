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
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

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
# The live collection stack (Phase 9 plan section 4, decision D1). A drill
# injects faults and stub traffic, and either would contaminate the audit the
# reliability evaluation reads, so no drill ever runs against it.
LIVE_PROJECT = "mp-live"

# D8 plants one audit row and D9 one constraint. Both carry a name no part of
# the pipeline writes, so a leftover is recognisable as this drill's and the
# baseline can refuse to start on top of one.
DRILL_D8_MARKER = "drill-d8 planted mismatch; deleted by the drill"
DRILL_D9_CONSTRAINT = "drill_d9_reject_one_cache_row"

# D5 has to kill the speed container *inside* a micro-batch. The batch's
# audit row is open only while the sinks are written, which on this stack is
# 10 ms for an empty batch and 90 ms for one crawl cycle, while a host-side
# `docker kill` needs about 220 ms to land. Killing inside a batch of that
# size is therefore luck, and a drill decided by luck is not evidence.
#
# So the drill makes the window instead of hunting for it: it stops the query,
# lets the crawler build a Kafka backlog, and kills inside the single batch
# that drains it. There is no `maxOffsetsPerTrigger`, so that batch is the
# whole backlog at once; 60 observations measured 268 ms, and the floor below
# leaves room for a slower machine as well as for the kill.
#
# Killing from inside the container is not an option: PID 1 *is* the driver,
# and Linux drops a default-action signal sent to a PID namespace's init from
# within that namespace, so `os.kill(1, 9)` there is silently ignored.
MID_BATCH_MIN_BACKLOG = 150
MID_BATCH_BACKLOG_TIMEOUT = 1500
MID_BATCH_POLL_SECONDS = 0.01


class DrillFailed(AssertionError):
    pass


class LiveStackRefused(RuntimeError):
    """The containers a drill would touch belong to the live collection stack."""


def container(service: str) -> str:
    """The container Compose runs ``service`` in.

    ``container_name`` carries ``${MP_CONTAINER_PREFIX}`` so that a bench or
    demo project can run beside the live one. Compose commands take service
    names; only the plain ``docker`` commands need this.
    """
    return os.environ.get("MP_CONTAINER_PREFIX", "") + service


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
        result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True,
                                env={**os.environ, **(env or {})})
        if check and result.returncode != 0:
            raise DrillFailed(f"{' '.join(args)} failed ({result.returncode}): {result.stderr.strip()[-500:]}")
        return result

    def up(self, *services: str, env: dict | None = None) -> None:
        self.run(COMPOSE + ["--profile", "*", "up", "-d", "--no-deps", *services], env=env)

    def stop(self, *services: str) -> None:
        self.run(COMPOSE + ["--profile", "*", "stop", *services])

    def kill(self, service: str, *, stays_dead: bool = False) -> None:
        """SIGKILL. The long-running services carry `restart: unless-stopped`,
        so Docker brings a killed one straight back; ``stays_dead`` clears that
        policy first, for a drill whose premise is a process that is gone."""
        if stays_dead:
            self.run(["docker", "update", "--restart", "no", container(service)])
        self.run(["docker", "kill", container(service)])

    def project(self) -> str:
        """This Compose project's name. A drill never touches another project's
        containers or volumes, so everything it removes is filtered by this."""
        cached = getattr(self, "_project", None)
        if cached:
            return cached
        label = '{{index .Config.Labels "com.docker.compose.project"}}'
        for service in ("speed", "minio", "postgres-dw", "kafka"):
            result = self.run(["docker", "inspect", "-f", label, container(service)], check=False)
            name = result.stdout.strip()
            if result.returncode == 0 and name and name != "<no value>":
                self._project = name
                return name
        # No container of this project is up: fall back to how Compose would
        # name it — the environment override, else the directory.
        self._project = os.environ.get("COMPOSE_PROJECT_NAME", "").strip() or ROOT.name.lower()
        return self._project

    def project_volumes(self, name_contains: str) -> list[str]:
        return self.run(["docker", "volume", "ls", "-q",
                         "--filter", f"label=com.docker.compose.project={self.project()}",
                         "--filter", f"name={name_contains}"]).stdout.split()

    def container_started_at(self, service: str) -> str:
        result = self.run(["docker", "inspect", "-f", "{{.State.StartedAt}}", container(service)], check=False)
        return result.stdout.strip() if result.returncode == 0 else "absent"

    def kill_when_batch_opens(self, since: datetime, *, timeout: float = 420) -> tuple[str, int]:
        """Wait for a micro-batch to open, then SIGKILL the container inside it.

        ``begin_batch`` inserts the row ``RUNNING`` before the sinks are
        written and only updates it afterwards, so the row is open for exactly
        as long as the batch's side effects take. The poll holds one
        connection open — about 2 ms a turn against the 20 ms a fresh one
        costs — because every millisecond here comes off the margin the kill
        itself needs. Returns the batch it aimed at; whether the kill actually
        landed inside it is decided afterwards, by the audit row.
        """
        from common.postgres import postgres_connection_factory

        deadline = time.monotonic() + timeout
        # One connection, held open for the whole poll: the factory hands out
        # a context manager, and its transaction is what keeps each read from
        # paying a fresh connection's 20 ms.
        with postgres_connection_factory()() as connection, connection.cursor() as cur:
            connection.autocommit = True
            while True:
                cur.execute("SELECT query_id, batch_id FROM audit.marketplace_speed_batch "
                            "WHERE status = 'RUNNING' AND started_at >= %s ORDER BY started_at DESC LIMIT 1",
                            (since,))
                row = cur.fetchone()
                if row:
                    self.kill("speed")
                    return row[0], row[1]
                if time.monotonic() >= deadline:
                    raise DrillFailed(f"no micro-batch opened within {timeout:.0f}s; there was nothing to kill inside")
                time.sleep(MID_BATCH_POLL_SECONDS)

    def observations_published_since(self, since: datetime) -> int:
        """Observations the crawler acknowledged to Kafka — the speed backlog."""
        from ops.smoke import smoke_targets

        (total,), = self.query(
            "SELECT coalesce(sum(a.parsed_count), 0) FROM audit.crawl_request_attempt a "
            "JOIN audit.crawl_frontier f USING (task_id) "
            "WHERE f.marketplace_code = 'tiki' AND f.target = ANY(%s) AND a.completed_at >= %s "
            "AND a.status IN ('SUCCEEDED', 'PARTIAL')", (smoke_targets(), since))
        return int(total)

    def stranded_speed_batches(self, since: datetime, *, stale_seconds: int) -> list[tuple]:
        """Micro-batches still RUNNING long after anything live would have settled."""
        return self.query(
            "SELECT query_id, batch_id, status, input_rows, change_rows, started_at FROM audit.marketplace_speed_batch "
            "WHERE started_at >= %s AND status = 'RUNNING' AND started_at < now() - %s * interval '1 second' "
            "ORDER BY started_at", (since, stale_seconds))

    def status(self, service: str) -> str:
        result = self.run(["docker", "inspect", "-f", "{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{end}}",
                           container(service)], check=False)
        return result.stdout.strip() if result.returncode == 0 else "absent"

    def wait_healthy(self, service: str, timeout: float = 180) -> None:
        wait_until(lambda: (self.status(service).endswith("/healthy"), self.status(service)),
                   timeout=timeout, what=f"{service} healthy", poll=3)

    def stub_mode(self, mode: str) -> None:
        code = ("import urllib.request,sys; urllib.request.urlopen(urllib.request.Request("
                "'http://localhost:8000/_stub/mode', data=sys.argv[1].encode()), timeout=10)")
        self.run(["docker", "exec", container("stub-source"), "python", "-c", code, mode])

    # -- PostgreSQL and the lake --
    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        return self.src.query(sql, params)

    def execute(self, sql: str, params: tuple = ()) -> None:
        """A statement with no result set. `query` always fetches, and DDL
        has nothing to fetch, which psycopg2 reports as "no results to fetch"."""
        from common.postgres import postgres_connection_factory

        with postgres_connection_factory()() as conn, conn.cursor() as cur:
            cur.execute(sql, params)

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

    def speed_batch(self, query_id: str, batch_id: int) -> tuple | None:
        """One micro-batch by its key. A retry updates this row in place, so
        the same (query_id, batch_id) is how a drill proves the *same* batch
        was retried rather than a later one quietly taking its place."""
        rows = self.query("SELECT query_id, batch_id, status, input_rows, change_rows, started_at, completed_at, "
                          "error_message FROM audit.marketplace_speed_batch WHERE query_id = %s AND batch_id = %s",
                          (query_id, batch_id))
        return rows[0] if rows else None

    def redis_recent_changes(self) -> list[str]:
        return self.src.redis_zset_members("rt:changes:recent")

    def wait_speed_drained(self, since: datetime, *, quiet_seconds: int = 75, timeout: float = 900) -> int:
        """Wait until the speed query has consumed everything Kafka holds.

        Its offsets live in the query's own checkpoint, not in a Kafka
        consumer group, so ``quiet()`` — which reads the Silver group's lag —
        says nothing about it, and a snapshot taken on the strength of that
        catches the query mid-topic. The one signal available is the audit:
        once no micro-batch has carried a row for longer than the trigger,
        there is nothing left to carry. Returns how many did.
        """
        def settled() -> tuple[bool, Any]:
            carried = [r for r in self.speed_batches_since(since) if r[3] > 0 or r[4] > 0]
            if not carried:
                return False, {"batches_with_rows": 0}
            idle = (datetime.now(timezone.utc) - carried[-1][5]).total_seconds()
            return idle > quiet_seconds, {"batches_with_rows": len(carried), "idle_seconds": round(idle)}

        observed = wait_until(settled, timeout=timeout, poll=15,
                              what=f"the speed query to go {quiet_seconds}s without a batch carrying rows")
        return observed["batches_with_rows"]

    # -- the batch --
    def plan_batch(self) -> dict:
        """The run_id and as_of the smoke would use for everything crawled so far."""
        plan = self.run(COMPOSE + ["--profile", "ops", "run", "--rm", "--no-deps", "ops", "python", "-m", "ops",
                                   "batch-plan", "--after-last-crawl"],
                        env={"MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS": str(BATCH_SETTLE_SECONDS)})
        return json.loads(plan.stdout.strip().splitlines()[-1])

    def batch_command(self, run_id: str, as_of: str, *extra: str) -> list[str]:
        return COMPOSE + ["--profile", "batch", "run", "--rm", "--no-deps", "batch-once", "python3", "-m",
                          "batch_layer.marketplace_warehouse", "--run-id", run_id, "--as-of", as_of, *extra]

    def run_batch(self, run_id: str, as_of: str, *extra: str) -> subprocess.CompletedProcess:
        return self.run(self.batch_command(run_id, as_of, *extra),
                        env={"MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS": str(BATCH_SETTLE_SECONDS)},
                        check=False)

    def start_batch(self, run_id: str, as_of: str, *extra: str) -> subprocess.Popen:
        """Launch a batch without waiting -- D10 needs two of them racing."""
        return subprocess.Popen(self.batch_command(run_id, as_of, *extra), cwd=ROOT,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                env={**os.environ,
                                     "MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS": str(BATCH_SETTLE_SECONDS)})

    def batch_after_last_crawl(self) -> tuple[int, str]:
        """One batch over everything crawled so far, as the smoke runs it."""
        plan = self.plan_batch()
        return self.run_batch(plan["run_id"], plan["as_of"]).returncode, plan["run_id"]

    def batch_run(self, run_id: str) -> tuple | None:
        rows = self.query("SELECT run_id, status, cache_published, error_message FROM audit.marketplace_batch_run "
                          "WHERE run_id = %s", (run_id,))
        return rows[0] if rows else None

    def quality_results(self, run_id: str) -> list[tuple]:
        return self.query("SELECT check_name, status, observed_value FROM audit.marketplace_quality_result "
                          "WHERE run_id = %s ORDER BY check_name", (run_id,))

    def serving_version(self) -> dict:
        """What is being served right now: the pointer, the cache version, its size."""
        pointer = self.src.current_pointer() or {}
        rows = self.query("SELECT run_id FROM audit.marketplace_cache_version WHERE singleton")
        (offers,), = self.query("SELECT count(*) FROM cache.marketplace_offer_current")
        return {"pointer_run_id": pointer.get("run_id"), "cache_run_id": rows[0][0] if rows else None,
                "cache_offer_rows": int(offers)}

    # -- the two documented mutations, each with its own reverse --
    def insert_mismatched_attempt(self, crawl_run_id: str, task_id: str, completed_at: datetime,
                                  parsed_count: int) -> int:
        """D8's one SQL statement: a settled attempt whose count Silver denies.

        Returns the BIGSERIAL key, which is how the drill later deletes exactly
        this row and nothing else.
        """
        (attempt_id,), = self.query(
            "INSERT INTO audit.crawl_request_attempt (crawl_run_id, task_id, attempt_number, started_at, "
            "completed_at, status, parsed_count, rejected_count, error_message) "
            "VALUES (%s, %s, %s, %s, %s, 'SUCCEEDED', %s, 0, %s) RETURNING attempt_id",
            (crawl_run_id, task_id, 99, completed_at, completed_at, parsed_count, DRILL_D8_MARKER))
        return int(attempt_id)

    def delete_attempt(self, attempt_id: int) -> list:
        return self.query("DELETE FROM audit.crawl_request_attempt WHERE attempt_id = %s RETURNING attempt_id",
                          (attempt_id,))

    def planted_attempts(self) -> list[tuple]:
        """Any D8 row still in the audit, by its marker. Nothing else matches."""
        return self.query("SELECT attempt_id FROM audit.crawl_request_attempt WHERE error_message = %s",
                          (DRILL_D8_MARKER,))

    def add_rejecting_constraint(self, offer_id: str) -> None:
        """D9's one SQL statement: refuse exactly one cache row at publication.

        ``NOT VALID`` so it binds the rows the next publish inserts without
        first rejecting the rows being served, which hold that same offer. The
        reverse is ``drop_rejecting_constraint``, called from the drill's
        ``finally``.
        """
        self.execute(f"ALTER TABLE cache.marketplace_offer_current ADD CONSTRAINT {DRILL_D9_CONSTRAINT} "
                     "CHECK (offer_id <> %s) NOT VALID", (offer_id,))

    def drop_rejecting_constraint(self) -> None:
        self.execute(f"ALTER TABLE cache.marketplace_offer_current DROP CONSTRAINT IF EXISTS {DRILL_D9_CONSTRAINT}")

    def constraint_exists(self, name: str) -> bool:
        (count,), = self.query("SELECT count(*) FROM pg_constraint WHERE conname = %s", (name,))
        return count > 0

    # -- D7 --
    def source_state(self) -> tuple:
        rows = self.query("SELECT consecutive_failures, opened_until FROM audit.crawl_source_state "
                          "WHERE marketplace_code = 'tiki'")
        return rows[0] if rows else (0, None)

    def attempt_count(self) -> int:
        (total,), = self.query("SELECT count(*) FROM audit.crawl_request_attempt")
        return int(total)

    def frontier_status(self, task_id: str) -> str:
        (status,), = self.query("SELECT status FROM audit.crawl_frontier WHERE task_id = %s", (task_id,))
        return status

    def kafka_end_offsets(self) -> dict:
        """End offset per partition of the observations topic: what reparse must not move."""
        import kafka

        from config.settings import KAFKA_BOOTSTRAP_SERVERS
        from config.topics import MARKETPLACE_OBSERVATIONS

        consumer = kafka.KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, consumer_timeout_ms=10000)
        try:
            partitions = consumer.partitions_for_topic(MARKETPLACE_OBSERVATIONS.name)
            if not partitions:
                raise DrillFailed(f"topic {MARKETPLACE_OBSERVATIONS.name} has no partitions")
            tps = [kafka.TopicPartition(MARKETPLACE_OBSERVATIONS.name, p) for p in sorted(partitions)]
            return {tp.partition: offset for tp, offset in consumer.end_offsets(tps).items()}
        finally:
            consumer.close()

    def silver_object_count(self) -> int:
        from config.settings import MARKETPLACE_SILVER_DATASET, data_lake_uri

        return len(self.src.list_objects(data_lake_uri("silver", MARKETPLACE_SILVER_DATASET)))

    # -- D11 only: a second Compose project, and JSON out of a tool container --
    @staticmethod
    def json_objects(text: str) -> list[dict]:
        """Every JSON object a tool container printed, compact or indented."""
        found, index = [], 0
        while True:
            start = text.find("{", index)
            if start == -1:
                return found
            depth, cursor = 0, start
            while cursor < len(text):
                depth += (text[cursor] == "{") - (text[cursor] == "}")
                cursor += 1
                if depth == 0:
                    break
            try:
                found.append(json.loads(text[start:cursor]))
                index = cursor
            except json.JSONDecodeError:
                index = start + 1

    def ops_in(self, project: str | None, *args: str) -> tuple[int, list[dict]]:
        """`python -m ops ...` in a project's ops container; returns its JSON."""
        command = list(COMPOSE)
        if project:
            command += ["-p", project]
        command += ["--profile", "ops", "run", "--rm", "--no-deps", "ops", "python", "-m", "ops", *args]
        result = self.run(command, check=False)
        return result.returncode, self.json_objects(result.stdout)

    def compose_in(self, project: str, *args: str, check: bool = True):
        return self.run(COMPOSE + ["-p", project, *args], check=check)

    def reparse(self, ref: Any) -> tuple[int, dict]:
        """Run `crawler.reparse` over one stored artifact, in the ops container.

        ``ref`` is a ``crawler.reparse.RawArtifactRef``.
        """
        result = self.run(COMPOSE + ["--profile", "ops", "run", "--rm", "--no-deps", "ops",
                                     "python", "-m", "crawler.reparse",
                                     "--marketplace", ref.marketplace_code, "--observed-date", ref.observed_date,
                                     "--hour", ref.hour, "--crawl-run-id", ref.crawl_run_id,
                                     "--raw-artifact-id", ref.raw_artifact_id], check=False)
        lines = [line for line in result.stdout.strip().splitlines() if line.startswith("{")]
        if not lines:
            raise DrillFailed(f"reparse printed no report (exit {result.returncode}): {result.stderr.strip()[-400:]}")
        return result.returncode, json.loads(lines[-1])

    def validate(self) -> list:
        from ops.validate import default_limits, validate

        return validate(self.src, default_limits())

    def es_change_ids(self) -> list[str]:
        from config.settings import ES_INDEX_MARKETPLACE_CHANGES

        return [doc["event_id"] for doc in self.src.es_documents(ES_INDEX_MARKETPLACE_CHANGES)]

    def es_changes_covering(self, members: Iterable[str], *, timeout: float = 60) -> list[str]:
        """Change ids in Elasticsearch, once every one of ``members`` is searchable.

        Elasticsearch is near-real-time: a bulk the sink has already committed
        — and whose Redis write therefore already happened — is not visible to
        a search until the next refresh, a second by default. Reading once the
        moment a batch is audited SUCCEEDED races that refresh and reports
        changes Redis holds and Elasticsearch "lost". The wait is bounded, so
        a change that really never arrives still fails the drill, naming it.
        """
        wanted, seen = set(members), []

        def searchable() -> tuple[bool, Any]:
            seen[:] = self.es_change_ids()
            missing = wanted - set(seen)
            return not missing, {"es_changes": len(seen), "not_searchable": len(missing),
                                 "examples": sorted(missing)[:3]}

        wait_until(searchable, timeout=timeout, poll=2,
                   what="Elasticsearch to make every change Redis holds searchable")
        return list(seen)


def _failing(results) -> list[str]:
    return [r.check for r in results if r.status != "PASS"]


def baseline(stack: Stack, rec: Record) -> None:
    failing = _failing(stack.validate())
    # D8 and D9 undo their mutation in a `finally`, but a killed process has no
    # `finally`. A leftover would quietly change what the next drill measures,
    # so it stops the drill instead, naming what to remove.
    leftovers = []
    if stack.planted_attempts():
        leftovers.append(f"{len(stack.planted_attempts())} planted audit row(s) from D8")
    if stack.constraint_exists(DRILL_D9_CONSTRAINT):
        leftovers.append(f"constraint {DRILL_D9_CONSTRAINT} from D9")
    rec.step("baseline", failing=failing, leftovers=leftovers)
    if failing:
        raise DrillFailed(f"baseline validate fails: {failing}; a drill never starts from a broken stack")
    if leftovers:
        raise DrillFailed(f"an earlier drill left the stack mutated: {leftovers}")


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
    """ES, then Redis, down under the speed query: one micro-batch fails, the
    query retries *that* batch, and it succeeds once the sink is back.

    The identity of the batch is the point. A later batch succeeding proves
    nothing: the audit row is keyed ``(query_name, query_id, batch_id)`` and a
    retry updates it in place, so the drill follows that one key from FAILED
    through its retry to SUCCEEDED.
    """
    for service in ("elasticsearch", "redis"):
        stack.stop(service)
        faulted = stack.db_now()
        rec.step("inject", stopped=service)
        _crawl_once(stack)
        failed = wait_until(lambda: ((b := [r for r in stack.speed_batches_since(faulted) if r[2] == "FAILED"]) != [], b),
                            timeout=420, what=f"a FAILED speed batch with {service} down")
        query_id, batch_id, first_attempt = failed[0][0], failed[0][1], failed[0][5]
        # Still faulted: the same row must go back to RUNNING under a newer
        # started_at. That is the query retrying this batch, not moving past it.
        retried = wait_until(
            lambda: ((row := stack.speed_batch(query_id, batch_id)) is not None and row[5] > first_attempt, row),
            timeout=420, what=f"batch {batch_id} of {query_id} to be retried with {service} still down")
        rec.step("observe", service=service, query_id=query_id, batch_id=batch_id,
                 first_attempt_at=first_attempt, retry_started_at=retried[5], retry_status=retried[2],
                 error=(retried[7] or "")[:300])
        stack.run(COMPOSE + ["up", "-d", service])
        stack.wait_healthy(service)
        rec.step("recover", started=service)
        ok = wait_until(lambda: ((row := stack.speed_batch(query_id, batch_id)) is not None and row[2] == "SUCCEEDED", row),
                        timeout=600, what=f"batch {batch_id} of {query_id} to succeed after {service} returns",
                        poll=10)
        recent = stack.redis_recent_changes()
        # Redis is written after the Elasticsearch bulk, so every recent change
        # must have a document; a retry that minted new ids would not. The wait
        # is for the search refresh, not for the write — see es_changes_covering.
        ids = stack.es_changes_covering(recent)
        rec.step("verify", service=service, query_id=query_id, batch_id=batch_id, completed_at=ok[6],
                 input_rows=ok[3], change_rows=ok[4], es_changes=len(ids), es_distinct=len(set(ids)),
                 redis_recent=len(recent), redis_distinct=len(set(recent)))
        if len(ids) != len(set(ids)):
            raise DrillFailed(f"duplicate change documents in Elasticsearch: {len(ids)} docs, {len(set(ids))} ids")
        if len(recent) != len(set(recent)):
            raise DrillFailed(f"duplicate members in rt:changes:recent: {len(recent)} members, {len(set(recent))} distinct")


def d5_speed_restart(stack: Stack, rec: Record) -> None:
    """Kill the query *inside* a micro-batch; then replay from a fresh checkpoint.

    Variant 1 has to interrupt a batch, not merely an idle query: a kill
    between triggers commits nothing and proves nothing. The window is made,
    not hunted — see MID_BATCH_MIN_BACKLOG. The proof the kill landed inside
    is the audit row left stranded in ``RUNNING`` with no ``completed_at``,
    which no live batch holds for more than a fraction of a second.
    """
    # 1. Stop the query and let the crawler fill Kafka, so the batch that
    #    drains the backlog is long enough to be killed inside.
    stack.stop("speed")
    since = stack.db_now()
    stack.start_crawler()
    backlog = wait_until(lambda: ((n := stack.observations_published_since(since)) >= MID_BATCH_MIN_BACKLOG, n),
                         timeout=MID_BATCH_BACKLOG_TIMEOUT, poll=15,
                         what=f"a backlog of {MID_BATCH_MIN_BACKLOG} observations for the speed query")
    stack.stop_crawler()
    before_kill = set(stack.es_change_ids())
    started_at = stack.container_started_at("speed")

    # 2. Start it again and kill it inside the batch that drains that backlog.
    stack.up("speed")
    query_id, batch_id = stack.kill_when_batch_opens(since)
    rec.step("inject", killed="speed", backlog_observations=backlog,
             mid_batch={"query_id": query_id, "batch_id": batch_id}, es_changes_before=len(before_kill))

    def interrupted() -> tuple[bool, Any]:
        row = stack.speed_batch(query_id, batch_id)
        open_for = (datetime.now(timezone.utc) - row[5]).total_seconds() if row else 0
        return bool(row and row[2] == "RUNNING" and open_for > 15), row

    stranded = wait_until(interrupted, timeout=240, poll=2,
                          what=f"batch {batch_id} to be left RUNNING by the kill; a terminal status here "
                               "means the kill landed between batches and interrupted nothing")
    rec.step("observe", stranded_batch={"query_id": query_id, "batch_id": batch_id,
                                        "status": stranded[2], "completed_at": stranded[6]})

    # 3. Recover. The interrupted batch is re-run under its own id, not skipped.
    stack.up("speed")                                  # a no-op if the restart policy got there first
    stack.wait_healthy("speed", timeout=300)
    restarted = stack.db_now()
    rec.step("recover", started="speed", container_started_at=stack.container_started_at("speed"), was=started_at)
    resumed = wait_until(lambda: ((row := stack.speed_batch(query_id, batch_id)) is not None and row[2] == "SUCCEEDED", row),
                         timeout=600, poll=5, what=f"the interrupted batch {batch_id} to be re-run and succeed")
    stack.start_crawler()
    wait_until(lambda: stack.every_target_succeeded_since(restarted), timeout=300, what="more crawls after the restart")
    stack.stop_crawler()
    stack.quiet()
    wait_until(lambda: (not stack.stranded_speed_batches(since, stale_seconds=90), None),
               timeout=420, poll=15, what="every micro-batch to reach a terminal status")
    stack.wait_speed_drained(since)
    recent = stack.redis_recent_changes()
    ids = stack.es_changes_covering(recent)
    lost = sorted(before_kill - set(ids))
    rec.step("verify", variant="kill", resumed_batch={"query_id": query_id, "batch_id": batch_id,
                                                      "input_rows": resumed[3], "change_rows": resumed[4]},
             es_changes=len(ids), distinct=len(set(ids)), before=len(before_kill), lost=len(lost))
    if len(ids) != len(set(ids)):
        raise DrillFailed(f"the restart duplicated change documents: {len(ids)} docs, {len(set(ids))} ids")
    if lost:
        raise DrillFailed(f"the kill lost {len(lost)} change event(s): {lost[:5]}")

    # Variant 2: no checkpoint at all. A full replay from earliest must yield
    # the same change documents, since every _id is deterministic.
    before = set(ids)
    stack.run(COMPOSE + ["--profile", "speed", "rm", "-sf", "speed"])
    # Scoped to this Compose project: another project's checkpoints are not
    # this drill's to delete, and a stale global match would silently make the
    # "replay" reuse a checkpoint that was never removed.
    volume = stack.project_volumes("speed_checkpoints")
    if not volume:
        raise DrillFailed(f"no speed_checkpoints volume in Compose project {stack.project()!r}; "
                          "a replay from a kept checkpoint would prove nothing")
    for name in volume:
        stack.run(["docker", "volume", "rm", name])
    replay_from = stack.db_now()
    stack.up("speed", env={"MARKETPLACE_STREAM_CHECKPOINT_VERSION": "drill-d5"})
    rec.step("inject", variant="replay", project=stack.project(), removed_volumes=volume,
             checkpoint_version="drill-d5")
    wait_until(lambda: (any(r[2] == "SUCCEEDED" and r[3] > 0 for r in stack.speed_batches_since(replay_from)), None),
               timeout=300, what="the replay's first batch")
    batches = stack.wait_speed_drained(replay_from)
    after = set(stack.es_changes_covering(stack.redis_recent_changes()))
    rec.step("verify", variant="replay", batches=batches, before=len(before), after=len(after),
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
    # Worker A is gone for good: the premise is that B, not a restarted A,
    # recovers the leases, so the restart policy is cleared before the kill.
    # `up` below recreates the container, and with it the policy from Compose.
    stack.kill("crawl-worker", stays_dead=True)
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


def _raw_artifact_ref(raw_uri: str):
    """The reference `crawler.reparse` addresses, read back off a stored URI.

    One place knows the Bronze layout, and it is the module that writes it:
    ``RawArtifactRef.from_uri`` is the inverse of ``RawArtifactRef.body_path``.
    This wrapper exists only to turn its refusal into a drill failure.
    """
    from crawler.reparse import RawArtifactRef

    try:
        return RawArtifactRef.from_uri(raw_uri)
    except ValueError as error:
        raise DrillFailed(str(error)) from error


def d7_parser_schema_drift(stack: Stack, rec: Record) -> None:
    """The source returns a shape the adapter must reject.

    A parse error is a fault in this response, not evidence the marketplace is
    unhealthy, so it is terminal for the task and must not move the source's
    circuit. The raw body is kept either way -- that is the whole point of
    raw-first -- and `crawler.reparse` over it must report PARSE_FAILED
    without writing anything anywhere.
    """
    circuit_before = stack.source_state()
    since = stack.db_now()
    stack.stub_mode("drift")
    rec.step("inject", stub_mode="drift", circuit_before=circuit_before)
    stack.start_crawler()
    rows = wait_until(lambda: ((r := [a for a in stack.attempts_since(since) if a[2] == "PARSE_ERROR"]) != [], r),
                      timeout=300, what="an attempt classified PARSE_ERROR")
    stack.stop_crawler()

    # Raw kept, task terminal, circuit untouched.
    kept = []
    for task_id, status, kind, parsed, raw_uri, *_ in rows:
        if raw_uri is None or not stack.src.object_exists(raw_uri):
            raise DrillFailed(f"a PARSE_ERROR attempt of {task_id} lost its raw body: {raw_uri}")
        kept.append((task_id, raw_uri))
    retried = [task for task, _ in kept if stack.frontier_status(task) not in ("FAILED", "DISABLED")]
    circuit_after = stack.source_state()
    rec.step("observe", parse_errors=len(rows), raw_kept=len(kept), circuit_after=circuit_after,
             still_schedulable=retried)
    if retried:
        raise DrillFailed(f"a parse error left {retried} schedulable; it is terminal for the attempt")
    if circuit_after[0] > circuit_before[0] or (circuit_after[1] is not None and circuit_before[1] is None):
        raise DrillFailed(f"a parse error moved the source circuit: {circuit_before} -> {circuit_after}")

    # Reparse the artifact just written. It must grade it PARSE_FAILED and
    # write nothing: no Kafka record, no Silver object, no audit attempt, and
    # neither the serving pointer nor the cache may move.
    ref = _raw_artifact_ref(kept[0][1])
    before = {"kafka": stack.kafka_end_offsets(), "silver": stack.silver_object_count(),
              "attempts": stack.attempt_count(), **stack.serving_version()}
    code, report = stack.reparse(ref)
    after = {"kafka": stack.kafka_end_offsets(), "silver": stack.silver_object_count(),
             "attempts": stack.attempt_count(), **stack.serving_version()}
    rec.step("observe", reparse_exit=code, reparse_counts=report.get("counts"), artifact=ref.raw_artifact_id,
             before=before, after=after)
    if report.get("counts", {}).get("PARSE_FAILED") != 1:
        raise DrillFailed(f"reparse of a drifted artifact did not report PARSE_FAILED: {report.get('counts')}")
    if code == 0:
        raise DrillFailed("reparse exited 0 over an artifact it could not parse")
    wrote = {key: (before[key], after[key]) for key in before if before[key] != after[key]}
    if wrote:
        raise DrillFailed(f"reparse is read-only and wrote: {wrote}")

    stack.stub_mode("ok")
    recovered_from = stack.db_now()
    rec.step("recover", stub_mode="ok")
    stack.start_crawler()
    wait_until(lambda: stack.every_target_succeeded_since(recovered_from), timeout=300,
               what="every target to succeed once the source returns a shape the adapter accepts")
    stack.stop_crawler()
    rec.step("verify", reparse="PARSE_FAILED", wrote_nothing=True, raw_kept=len(kept))


def _settled_crawl_run(stack: Stack, as_of: datetime) -> tuple[str, str, datetime]:
    """A crawl run inside the reconciliation window that Silver already holds.

    The gate reconciles only runs present in Silver, settled at least the
    settle delay before ``as_of`` and no older than the lookback, so a planted
    mismatch is only seen if it joins one of those.
    """
    from config.settings import MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS
    from ops.smoke import smoke_targets

    newest = as_of - timedelta(seconds=BATCH_SETTLE_SECONDS)
    oldest = as_of - timedelta(seconds=MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS)
    rows = stack.query(
        "SELECT a.crawl_run_id, a.task_id, r.completed_at FROM audit.crawl_request_attempt a "
        "JOIN audit.crawl_run r USING (crawl_run_id) JOIN audit.crawl_frontier f ON f.task_id = a.task_id "
        "WHERE f.marketplace_code = 'tiki' AND f.target = ANY(%s) AND a.status IN ('SUCCEEDED', 'PARTIAL') "
        "AND a.parsed_count > 0 AND r.completed_at IS NOT NULL AND r.completed_at <= %s AND r.completed_at >= %s "
        "ORDER BY r.completed_at DESC LIMIT 1", (smoke_targets(), newest, oldest))
    if not rows:
        raise DrillFailed(f"no settled crawl run between {oldest} and {newest} to disagree with")
    return rows[0][0], rows[0][1], rows[0][2]


def d8_quality_failure(stack: Stack, rec: Record) -> None:
    """One audit row Silver denies: the gate refuses, and nothing moves.

    The run still records its evidence -- a refused run with no stored results
    cannot be told apart from a crash -- but the cache version and the serving
    pointer stay where they were, so the failed run's Gold never becomes the
    version anyone reads. Deleting that one row and resuming the same run must
    then publish normally.
    """
    before = stack.serving_version()
    _crawl_once(stack)
    stack.quiet()
    plan = stack.plan_batch()
    as_of = datetime.fromisoformat(plan["as_of"].replace("Z", "+00:00"))
    crawl_run_id, task_id, settled_at = _settled_crawl_run(stack, as_of)
    attempt_id = None
    try:
        attempt_id = stack.insert_mismatched_attempt(crawl_run_id, task_id, settled_at, parsed_count=7)
        rec.step("inject", attempt_id=attempt_id, crawl_run_id=crawl_run_id, parsed_count=7,
                 settled_at=settled_at, run_id=plan["run_id"], as_of=plan["as_of"], before=before)

        refused = stack.run_batch(plan["run_id"], plan["as_of"])
        row = stack.batch_run(plan["run_id"])
        results = stack.quality_results(plan["run_id"])
        failing = [name for name, status, _ in results if status != "PASS"]
        during = stack.serving_version()
        rec.step("observe", exit_code=refused.returncode, status=None if row is None else row[1],
                 cache_published=None if row is None else row[2], quality_results=len(results),
                 failing_checks=failing, serving=during)
        if refused.returncode == 0 or row is None or row[1] != "QUALITY_FAILED":
            raise DrillFailed(f"the gate let a denied window through: exit {refused.returncode}, run {row}")
        if "silver_parse_attempt_reconciliation" not in failing:
            raise DrillFailed(f"the run failed for the wrong reason: {failing}")
        if not results:
            raise DrillFailed("a refused run stored no quality results; a block with no evidence is not a block")
        if during != before:
            raise DrillFailed(f"a refused run moved the serving version: {before} -> {during}")
    finally:
        if attempt_id is not None:
            stack.delete_attempt(attempt_id)
    left = stack.planted_attempts()
    rec.step("recover", deleted_attempt=attempt_id, planted_rows_left=len(left))
    if left:
        raise DrillFailed(f"the planted audit row outlived the drill: {left}")

    resumed = stack.run_batch(plan["run_id"], plan["as_of"], "--resume")
    row = stack.batch_run(plan["run_id"])
    after = stack.serving_version()
    rec.step("verify", exit_code=resumed.returncode, status=None if row is None else row[1], serving=after)
    if resumed.returncode != 0 or row is None or row[1] != "SUCCEEDED":
        raise DrillFailed(f"the resume did not succeed: exit {resumed.returncode}, run {row}")
    if after["pointer_run_id"] != plan["run_id"] or after["cache_run_id"] != plan["run_id"]:
        raise DrillFailed(f"the pointer advanced only on the successful resume, so both must name "
                          f"{plan['run_id']}: {after}")


def d9_publish_failure(stack: Stack, rec: Record) -> None:
    """PostgreSQL refuses one cache row at publication.

    The publish truncates the serving tables and refills them in one
    transaction, so a refusal must take the truncate with it. What is already
    being served -- the cache rows, the cache version and the pointer -- has to
    survive untouched, and a resume of the same run must then publish it.
    """
    before = stack.serving_version()
    rows = stack.query("SELECT offer_id FROM cache.marketplace_offer_current ORDER BY offer_id LIMIT 1")
    if not rows:
        raise DrillFailed("the cache serves no offer, so there is no row for the constraint to refuse")
    offer_id = rows[0][0]
    _crawl_once(stack)
    stack.quiet()
    plan = stack.plan_batch()
    try:
        stack.add_rejecting_constraint(offer_id)
        rec.step("inject", constraint=DRILL_D9_CONSTRAINT, rejected_offer=offer_id,
                 run_id=plan["run_id"], as_of=plan["as_of"], before=before)

        failed = stack.run_batch(plan["run_id"], plan["as_of"])
        row = stack.batch_run(plan["run_id"])
        during = stack.serving_version()
        message = (row[3] or "") if row else ""
        rec.step("observe", exit_code=failed.returncode, status=None if row is None else row[1],
                 cache_published=None if row is None else row[2], serving=during,
                 error=message[:300])
        if failed.returncode == 0 or row is None or row[1] != "FAILED":
            raise DrillFailed(f"publication was refused but the run did not fail: exit {failed.returncode}, {row}")
        if DRILL_D9_CONSTRAINT not in message:
            raise DrillFailed(f"the run failed at something other than the constraint: {message[:300]}")
        if row[2]:
            raise DrillFailed("a run that could not publish is recorded as having published")
        if during != before:
            raise DrillFailed(f"the refused transaction did not roll back whole: {before} -> {during}")
    finally:
        stack.drop_rejecting_constraint()
    rec.step("recover", constraint_dropped=DRILL_D9_CONSTRAINT,
             still_present=stack.constraint_exists(DRILL_D9_CONSTRAINT))
    if stack.constraint_exists(DRILL_D9_CONSTRAINT):
        raise DrillFailed("the drill's constraint outlived it")

    resumed = stack.run_batch(plan["run_id"], plan["as_of"], "--resume")
    row = stack.batch_run(plan["run_id"])
    after = stack.serving_version()
    rec.step("verify", exit_code=resumed.returncode, status=None if row is None else row[1], serving=after)
    if resumed.returncode != 0 or row is None or row[1] != "SUCCEEDED":
        raise DrillFailed(f"the resume did not succeed: exit {resumed.returncode}, run {row}")
    if after["pointer_run_id"] != plan["run_id"] or after["cache_run_id"] != plan["run_id"]:
        raise DrillFailed(f"after recovery the pointer and the cache must both name {plan['run_id']}: {after}")


def d10_concurrent_batches(stack: Stack, rec: Record) -> None:
    """Two batches at once: one runs, the other is refused having done nothing.

    The two carry different run ids on purpose. With one id the loser's
    "wrote no audit row" would be indistinguishable from the winner's row, and
    the exit code alone is not evidence; with two, the refused id must have no
    row in `audit.marketplace_batch_run` at all.
    """
    _crawl_once(stack)
    stack.quiet()
    plan = stack.plan_batch()
    contender = f"{plan['run_id']}-d10"
    if stack.batch_run(contender) is not None:
        raise DrillFailed(f"{contender} already has an audit row; a previous D10 did not clean up")
    first = stack.start_batch(plan["run_id"], plan["as_of"])
    second = stack.start_batch(contender, plan["as_of"])
    rec.step("inject", started=[plan["run_id"], contender], as_of=plan["as_of"])
    outcomes = {}
    for run_id, process in ((plan["run_id"], first), (contender, second)):
        stdout, stderr = process.communicate(timeout=1800)
        outcomes[run_id] = {"exit": process.returncode,
                            "stdout": stdout.strip().splitlines()[-1][:300] if stdout.strip() else "",
                            "stderr": stderr.strip()[-200:]}
    refused = [run_id for run_id, out in outcomes.items() if out["exit"] == 75]
    ran = [run_id for run_id, out in outcomes.items() if out["exit"] == 0]
    rec.step("observe", outcomes=outcomes, refused=refused, ran=ran)
    if len(refused) != 1 or len(ran) != 1:
        raise DrillFailed(f"exactly one batch must run and one be refused 75: {outcomes}")
    loser, winner = refused[0], ran[0]
    if "ALREADY_RUNNING" not in outcomes[loser]["stdout"]:
        raise DrillFailed(f"the refused run did not say ALREADY_RUNNING: {outcomes[loser]}")

    # The audit database, not the exit code, is what decides this.
    loser_row = stack.batch_run(loser)
    winner_row = stack.batch_run(winner)
    serving = stack.serving_version()
    versions = stack.query("SELECT count(*) FROM audit.marketplace_cache_version")[0][0]
    rec.step("verify", winner=winner, loser=loser, loser_audit_row=loser_row,
             winner_status=None if winner_row is None else winner_row[1], cache_versions=int(versions),
             serving=serving)
    if loser_row is not None:
        raise DrillFailed(f"the refused run wrote an audit row: {loser_row}")
    if winner_row is None or winner_row[1] != "SUCCEEDED":
        raise DrillFailed(f"the run that took the lock did not succeed: {winner_row}")
    if int(versions) != 1:
        raise DrillFailed(f"the cache version is a singleton; found {versions} rows")
    if serving["pointer_run_id"] != winner or serving["cache_run_id"] != winner:
        raise DrillFailed(f"the pointer and the cache must both name the run that won the lock, {winner}: {serving}")


def _backup_root() -> Path:
    return ROOT / "data" / "ops" / "backups"


def d11_backup_and_restore(stack: Stack, rec: Record) -> None:
    """Back the stack up, then restore it into a *different* Compose project.

    The one drill whose fault is not an outage: it asks whether the evidence
    survives losing the machine. It is therefore also the only drill that
    takes the stack down -- `container_name` is global and two projects of
    this Compose file cannot be up at once -- which is why it runs last, and
    why its `finally` removes the restored project before the harness brings
    the real one back.

    Two things it proves that an exit code alone would not: the restore wrote
    into its own volumes and never touched the running stack's, and the
    restored pointer serves the version the backup named rather than merely
    some coherent version of its own.
    """
    project = stack.project()
    target = f"{project}-restore"
    if target == project:
        raise DrillFailed("the restore project must differ from the running one")
    before = stack.serving_version()
    volumes_before = sorted(stack.project_volumes(""))

    code, reports = stack.ops_in(None, "backup")
    written = next((r for r in reports if r.get("event") == "backup_written"), None)
    if code != 0 or written is None:
        raise DrillFailed(f"backup did not complete (exit {code})")
    backup_id = written["backup_id"]
    manifest = json.loads((_backup_root() / backup_id / "backup-manifest.json").read_text(encoding="utf-8"))
    rec.step("inject", action="backup", backup_id=backup_id, counts=written["counts"],
             files=written["files"], pointer_run_id=manifest["pointer_run_id"],
             cache_run_id=manifest["cache_run_id"])
    # The backup refuses itself when these two disagree. Assert the record
    # says so rather than trusting that the refusal would have fired.
    served = {manifest["pointer_run_id"], manifest["cache_run_id"], before["pointer_run_id"]}
    if len(served) != 1:
        raise DrillFailed(f"the backup's run ids disagree with what was being served: {sorted(served)}")

    try:
        stack.run(COMPOSE + ["--profile", "*", "down"])
        stack.compose_in(target, "up", "-d")
        stack.wait_healthy("postgres-dw")
        if stack.ops_in(target, "migrate")[0] != 0:
            raise DrillFailed("the restored project could not be migrated")

        code, reports = stack.ops_in(target, "restore", "--from", backup_id)
        report = next((r for r in reports if "checks" in r), {"checks": []})
        statuses = {c["check"]: c["status"] for c in report["checks"]}
        plan = next((r for r in reports if r.get("event") == "quality_only_batch_next"), {})
        rec.step("observe", restore_exit=code, checks=statuses, as_of=plan.get("as_of"))
        if code != 0 or len(statuses) != 3 or set(statuses.values()) != {"PASS"}:
            raise DrillFailed(f"the restore checks did not all pass: {statuses}")

        # Plan 12.3 step 3's fourth check. It needs Spark, so it runs the way
        # every other Spark job does, in its own container.
        run_id = f"{plan['pointer_run_id']}-d11"[:64]
        result = stack.compose_in(target, "--profile", "batch", "run", "--rm", "--no-deps", "batch-once",
                                  "python3", "-m", "batch_layer.marketplace_warehouse",
                                  "--run-id", run_id, "--as-of", plan["as_of"], "--quality-only", check=False)
        batch = next((r for r in stack.json_objects(result.stdout) if "quality_status" in r), {})
        rec.step("observe", quality_only=run_id, quality_status=batch.get("quality_status"),
                 mandatory_failures=batch.get("mandatory_failure_count"),
                 manifest_promoted=batch.get("manifest_promoted"), silver_rows=batch.get("silver_rows"))
        if result.returncode != 0 or batch.get("quality_status") != "PASS" or batch.get("mandatory_failure_count"):
            raise DrillFailed(f"the quality gate refused the restored Silver: {batch}")
        if batch.get("manifest_promoted"):
            raise DrillFailed("a --quality-only run promoted the pointer")

        code, reports = stack.ops_in(target, "validate", "--restored")
        verdict = next((r for r in reports if r.get("event") == "validate_restored"), {})
        checks = next((r for r in reports if "checks" in r), {"checks": []})
        pointer = next((c for c in checks["checks"] if c["check"] == "pointer_matches_cache"), {})
        rec.step("observe", validate_restored=verdict.get("passed"), tolerated=verdict.get("tolerated"),
                 unexpected=verdict.get("unexpected"), restored_pointer=pointer.get("observed"))
        if code != 0 or not verdict.get("passed") or verdict.get("unexpected"):
            raise DrillFailed(f"the restored stack fails a check that is not an empty derived store: {verdict}")
        if (pointer.get("observed") or {}).get("pointer") != manifest["pointer_run_id"]:
            raise DrillFailed(f"the restored pointer serves {pointer.get('observed')}, "
                              f"not the backed-up {manifest['pointer_run_id']}")
    finally:
        # Before the harness brings the real stack back: the two projects
        # cannot both own these container names.
        stack.compose_in(target, "--profile", "*", "down", "--volumes", check=False)

    volumes_after = sorted(stack.project_volumes(""))
    rec.step("verify", restored_into=target, backup_id=backup_id,
             running_project_volumes=len(volumes_after), intact=volumes_after == volumes_before)
    # "Never restore over the running stack" is the rule; this is what it
    # means in practice -- the running project's volumes were never an input.
    if volumes_after != volumes_before:
        raise DrillFailed("the running project's volumes changed across a restore: "
                          f"{sorted(set(volumes_before) ^ set(volumes_after))}")


DRILLS: dict[str, Callable[[Stack, Record], None]] = {
    "d1": d1_source_errors,
    "d2": d2_kafka_down_during_crawl,
    "d3": d3_minio_down_during_sink,
    "d4": d4_sinks_down_during_speed,
    "d5": d5_speed_restart,
    "d6": d6_expired_lease,
    "d7": d7_parser_schema_drift,
    "d8": d8_quality_failure,
    "d9": d9_publish_failure,
    "d10": d10_concurrent_batches,
    # Last on purpose: the only drill that takes the stack down.
    "d11": d11_backup_and_restore,
}


def refuse_live(stack: Stack) -> None:
    """Refuse before anything is touched -- the baseline, the drill *and* the
    restore in ``run``'s ``finally`` all act on the stack. Decided by the
    project label of the containers themselves, not by an environment variable
    someone might have forgotten to load."""
    if stack.project() == LIVE_PROJECT:
        raise LiveStackRefused(
            f"the containers under prefix '{os.environ.get('MP_CONTAINER_PREFIX', '')}' belong to "
            f"'{LIVE_PROJECT}', the live collection stack; run drills with -EnvFile env/bench.env")


def run(name: str, stack: Stack | None = None) -> Record:
    stack = stack or Stack()
    refuse_live(stack)
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
    try:
        refuse_live(Stack())
    except LiveStackRefused as error:
        print(json.dumps({"event": "drill_refused", "reason": str(error)}), flush=True)
        return 2
    results = {name: run(name).passed for name in names}
    print(json.dumps({"drills": results}, sort_keys=True))
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())

"""The waits inside `mp smoke` — Phase 8 plan section 9.1.

`scripts/mp.ps1 smoke` brings the profiles up, migrates and seeds, then calls
``python -m ops smoke-wait``, which polls until the streaming half of the slice
has visibly run:

1. every smoke target has been crawled successfully at least twice since the smoke started, so
   the stub served each page at two prices;
2. Silver holds exactly the observations those attempts parsed;
3. the speed audit has a ``SUCCEEDED`` micro-batch with ``change_rows > 0``.

The smoke has its own crawl universe (``SMOKE_CATEGORIES``), never a real Tiki
category, on the ACTIVE tier. A stack reused across smokes keeps those tasks;
counting only attempts since the smoke began keeps every run honest.

``since`` is PostgreSQL's ``now()`` when the wait starts, so every comparison
is between instants the stack recorded itself.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Callable

SMOKE_CATEGORIES = ("9001", "9002", "9003")
SMOKE_PAGES = 2
SUCCEEDED_ATTEMPTS = ("SUCCEEDED", "PARTIAL")


def smoke_targets(categories=SMOKE_CATEGORIES, pages: int = SMOKE_PAGES) -> list[str]:
    """The frontier targets of the smoke universe.

    Targets, not task IDs: each success schedules the next crawl of a target as
    a new task, so the seeded task IDs only ever see one attempt each.
    """
    from crawler.contracts import encode_listing_page_task_target

    return [encode_listing_page_task_target(category, page) for category in categories for page in range(1, pages + 1)]


_PENDING = "('READY', 'RETRY_WAIT', 'LEASED')"


def park(connection_factory) -> int:
    """Disable every pending smoke task; return how many.

    The smoke's categories are made up, and each success schedules the next
    crawl. Left pending, a later `mp up` with the real TIKI_LISTING_URL would
    send them to the live marketplace, and their runs would feed Silver and
    Gold. mp.ps1 parks them whenever a smoke ends, passed or failed.
    """
    with connection_factory() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE audit.crawl_frontier SET status = 'DISABLED', lease_owner = NULL, lease_expires_at = NULL, "
            f"updated_at = now() WHERE marketplace_code = 'tiki' AND target = ANY(%s) AND status IN {_PENDING}",
            (smoke_targets(),))
        return cur.rowcount


def activate(connection_factory) -> int:
    """Re-enable parked smoke tasks, due now; return how many."""
    with connection_factory() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE audit.crawl_frontier SET status = 'READY', scheduled_for = now(), updated_at = now() "
            "WHERE marketplace_code = 'tiki' AND target = ANY(%s) AND status = 'DISABLED'",
            (smoke_targets(),))
        return cur.rowcount


@dataclass(frozen=True)
class Condition:
    name: str
    met: bool
    observed: Any


_SMOKE_ATTEMPTS = ("FROM audit.crawl_request_attempt a JOIN audit.crawl_frontier f USING (task_id) "
                   "WHERE f.marketplace_code = 'tiki' AND f.target = ANY(%s) AND a.started_at >= %s")


def crawl_condition(src, *, targets, since) -> Condition:
    rows = dict(src.query(
        f"SELECT f.target, count(*) {_SMOKE_ATTEMPTS} AND a.status = ANY(%s) GROUP BY 1",
        (list(targets), since, list(SUCCEEDED_ATTEMPTS))))
    counts = {target: int(rows.get(target, 0)) for target in targets}
    return Condition("each_task_succeeded_twice", all(n >= 2 for n in counts.values()), sorted(counts.values()))


def silver_condition(src, *, targets, since) -> Condition:
    from config.settings import MARKETPLACE_SILVER_DATASET, data_lake_uri

    parsed = dict(src.query(
        f"SELECT a.crawl_run_id, sum(a.parsed_count) {_SMOKE_ATTEMPTS} GROUP BY 1", (list(targets), since)))
    expected = sum(int(n) for n in parsed.values())
    found = 0
    for uri in src.list_objects(data_lake_uri("silver", MARKETPLACE_SILVER_DATASET)):
        payload = src.read_object(uri)
        if payload is not None and json.loads(payload).get("crawl_run_id") in parsed:
            found += 1
    return Condition("silver_holds_parsed_observations", expected > 0 and found == expected,
                     {"expected": expected, "found": found})


def speed_condition(src, *, since) -> Condition:
    (count,), = src.query(
        "SELECT count(*) FROM audit.marketplace_speed_batch WHERE status = 'SUCCEEDED' AND change_rows > 0 "
        "AND started_at >= %s", (since,))
    return Condition("speed_emitted_changes", int(count) > 0, int(count))


def wait(
    src,
    *,
    targets,
    timeout_seconds: float,
    poll_seconds: float = 10,
    log: Callable[[str], None] = print,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    since=None,
) -> bool:
    """Poll until all three conditions hold, or the timeout passes; True if they held."""
    if since is None:
        (since,), = src.query("SELECT now()")
    deadline = monotonic() + timeout_seconds
    while True:
        conditions = [crawl_condition(src, targets=targets, since=since),
                      silver_condition(src, targets=targets, since=since),
                      speed_condition(src, since=since)]
        log(json.dumps({"event": "smoke_wait", "conditions": {c.name: {"met": c.met, "observed": c.observed} for c in conditions}},
                       sort_keys=True, default=str))
        if all(c.met for c in conditions):
            return True
        if monotonic() >= deadline:
            return False
        sleep(poll_seconds)


def wait_for_quiet(
    src,
    *,
    timeout_seconds: float,
    poll_seconds: float = 5,
    log: Callable[[str], None] = print,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> bool:
    """With the crawler stopped, wait until the Silver sink has consumed everything.

    The quiet period of plan 9.2: validate's lag check is only meaningful once
    nothing new is being published.
    """
    from config.settings import KAFKA_SILVER_CONSUMER_GROUP
    from config.topics import MARKETPLACE_OBSERVATIONS

    deadline = monotonic() + timeout_seconds
    while True:
        lag = src.consumer_lag(KAFKA_SILVER_CONSUMER_GROUP, MARKETPLACE_OBSERVATIONS.name)
        log(json.dumps({"event": "smoke_quiet", "silver_lag": lag}))
        if lag == 0:
            return True
        if monotonic() >= deadline:
            return False
        sleep(poll_seconds)

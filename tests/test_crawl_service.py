"""Crawl service and frontier seeding, Phase 8 plan section 6.1, items 1-7.

A scripted worker and recording fakes stand in for PostgreSQL and Kafka. No
test opens a socket, and none sleeps: the service waits on a stop signal, and
every wait here returns at once.
"""
import signal
import subprocess
import sys
from datetime import timedelta

import pytest

from config.marketplace_schema import ResourceType
from crawler import seed_frontier, service
from crawler.contracts import AcquisitionStatus, ObservationPublishError, decode_listing_page_task_target
from crawler.scheduling import CrawlTaskStatus, CrawlTier, make_crawl_task_id
from crawler.worker import CycleResult
from tests.test_crawl_worker import NOW, FakeReport, _task


def _cycle(leased):
    return CycleResult(leased=leased, succeeded=leased, retried=0, failed=0, skipped_circuit_open=0, recovered_leases=0)


class ScriptedWorker:
    def __init__(self, leased_per_cycle, *, on_cycle=None):
        self.leased = list(leased_per_cycle)
        self.on_cycle = on_cycle or (lambda number: None)
        self.cycles = 0
        self.run_ids = []

    def run_once(self, *, crawl_run_id_for):
        self.cycles += 1
        self.run_ids.append(crawl_run_id_for(_task()))
        self.on_cycle(self.cycles)
        return _cycle(self.leased[self.cycles - 1])


class RecordingStop:
    """A stop signal whose waits are recorded and return at once."""

    def __init__(self):
        self.waits = []
        self.stopped = False

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, seconds):
        self.waits.append(seconds)
        return self.stopped


def _serve(worker, stop, **overrides):
    options = {"idle_seconds": 30, "poll_seconds": 5, "max_cycles": None, "log": lambda line: None}
    options.update(overrides)
    return service.run_service(worker, stop=stop, **options)


# 1
def test_the_service_waits_the_idle_interval_only_when_nothing_was_leased():
    worker, stop = ScriptedWorker([0, 3, 0]), RecordingStop()

    _serve(worker, stop, max_cycles=3)

    assert worker.cycles == 3
    # No wait after the last cycle: the bound is reached, so the service ends.
    assert stop.waits == [30, 5]


# 1
def test_each_cycle_is_logged_as_one_json_line():
    import json

    lines = []
    _serve(ScriptedWorker([2]), RecordingStop(), max_cycles=1, log=lines.append)

    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["event"] == "crawl_cycle"
    assert record["leased"] == 2


# 2
def test_a_stop_request_finishes_the_cycle_in_hand_and_starts_no_other():
    stop = RecordingStop()
    worker = ScriptedWorker([2, 2, 2], on_cycle=lambda number: stop.set())

    _serve(worker, stop)

    assert worker.cycles == 1


# 2
def test_a_stop_during_the_wait_ends_the_service_without_another_cycle():
    class StopWhileWaiting(RecordingStop):
        def wait(self, seconds):
            self.stopped = True
            return super().wait(seconds)

    worker, stop = ScriptedWorker([0, 0]), StopWhileWaiting()

    _serve(worker, stop)

    assert worker.cycles == 1


# 2
def test_sigterm_and_sigint_set_the_stop_signal():
    stop = service.StopSignal()
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        service.install_signal_handlers(stop)
        for sig in previous:
            handler = signal.getsignal(sig)
            assert callable(handler)
        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        assert stop.is_set()
        # A set signal makes the next wait return at once, not after its timeout.
        assert stop.wait(3600) is True
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


# 3
def test_every_observation_is_published_in_order_and_the_report_returned_unchanged():
    report = FakeReport(observations=("obs-a", "obs-b", "obs-c"))
    sent = []
    execute = service.publishing_executor(lambda *, task, crawl_run_id: report, publish=sent.append)

    assert execute(task=_task(), crawl_run_id="run-1") is report
    assert sent == ["obs-a", "obs-b", "obs-c"]


# 3
def test_a_failed_acquisition_is_never_published():
    report = FakeReport(status=AcquisitionStatus.FAILED, http_status=500, observations=("obs-a",))
    sent = []
    execute = service.publishing_executor(lambda *, task, crawl_run_id: report, publish=sent.append)

    assert execute(task=_task(), crawl_run_id="run-1") is report
    assert sent == []


# 3
def test_an_acquisition_error_passes_through_unpublished():
    def broken(*, task, crawl_run_id):
        raise TimeoutError("source timed out")

    sent = []
    execute = service.publishing_executor(broken, publish=sent.append)

    with pytest.raises(TimeoutError):
        execute(task=_task(), crawl_run_id="run-1")
    assert sent == []


# 4
def test_a_failed_publish_reports_how_many_observations_were_acknowledged():
    report = FakeReport(observations=("obs-a", "obs-b", "obs-c", "obs-d"))
    sent = []

    def publish(event):
        if event == "obs-c":
            raise RuntimeError("KafkaTimeoutError: broker unavailable")
        sent.append(event)

    execute = service.publishing_executor(lambda *, task, crawl_run_id: report, publish=publish)

    with pytest.raises(ObservationPublishError) as caught:
        execute(task=_task(), crawl_run_id="run-1")

    assert caught.value.acknowledged == 2
    assert caught.value.report is report
    assert isinstance(caught.value.__cause__, RuntimeError)
    # Nothing after the failure is attempted: order is what makes the count exact.
    assert sent == ["obs-a", "obs-b"]


# 4
def test_a_publish_error_refuses_an_impossible_acknowledged_count():
    report = FakeReport(observations=("obs-a",))

    with pytest.raises(ValueError, match="acknowledged"):
        ObservationPublishError(report=report, acknowledged=2, cause=RuntimeError("x"))
    with pytest.raises(ValueError, match="acknowledged"):
        ObservationPublishError(report=report, acknowledged=-1, cause=RuntimeError("x"))


# 6
def test_each_attempt_of_a_task_gets_its_own_crawl_run_id():
    first = _task(attempts=1)
    retried = _task(attempts=2, lease_expires_at=NOW + timedelta(minutes=40))

    assert service.crawl_run_id_for(first) == service.crawl_run_id_for(first)
    assert service.crawl_run_id_for(first) != service.crawl_run_id_for(retried)
    # The same attempt number under a fresh lease (a crash, then recovery) is
    # a different run too.
    relet = _task(attempts=1, lease_expires_at=NOW + timedelta(minutes=10))
    assert service.crawl_run_id_for(first) != service.crawl_run_id_for(relet)
    # audit.crawl_run.crawl_run_id is VARCHAR(128).
    assert len(service.crawl_run_id_for(first)) <= 128


def test_the_service_hands_the_worker_the_attempt_scoped_run_id():
    worker = ScriptedWorker([1])

    _serve(worker, RecordingStop(), max_cycles=1)

    assert worker.run_ids == [service.crawl_run_id_for(_task())]


def test_the_default_worker_id_is_unique_per_process():
    worker_id = service.default_worker_id()

    assert worker_id.endswith(f":{__import__('os').getpid()}")
    assert worker_id.split(":")[0]


def test_the_service_refuses_non_positive_intervals():
    with pytest.raises(ValueError, match="idle_seconds"):
        _serve(ScriptedWorker([0]), RecordingStop(), max_cycles=1, idle_seconds=0)
    with pytest.raises(ValueError, match="poll_seconds"):
        _serve(ScriptedWorker([0]), RecordingStop(), max_cycles=1, poll_seconds=0)
    with pytest.raises(ValueError, match="max_cycles"):
        _serve(ScriptedWorker([0]), RecordingStop(), max_cycles=0)


# ----------------------------------------------------------------------------
# Frontier seeding.
# ----------------------------------------------------------------------------
class RecordingFrontier:
    def __init__(self):
        self.rows = {}

    def enqueue(self, task):
        if task.task_id in self.rows:
            return False
        self.rows[task.task_id] = task
        return True


def _seed(frontier, **overrides):
    options = {
        "marketplace_code": "tiki", "marketplace_id": "marketplace-tiki",
        "categories": ["1846", "8322"], "pages": 2,
        "tier": CrawlTier.NORMAL, "priority": 0, "max_attempts": 5,
    }
    options.update(overrides)
    return seed_frontier.seed(frontier, **options)


# 7
def test_seeding_enqueues_one_ready_task_per_category_page():
    frontier = RecordingFrontier()

    assert _seed(frontier) == 4

    targets = sorted(decode_listing_page_task_target(task.target) for task in frontier.rows.values())
    assert targets == [("1846", 1), ("1846", 2), ("8322", 1), ("8322", 2)]
    for task in frontier.rows.values():
        assert task.status is CrawlTaskStatus.READY
        assert task.attempts == 0
        assert task.resource_type is ResourceType.LISTING_PAGE
        assert task.scheduled_for == seed_frontier.SEED_SCHEDULED_FOR


# 7
def test_seeding_the_same_targets_again_enqueues_nothing():
    frontier = RecordingFrontier()

    _seed(frontier)

    assert _seed(frontier) == 0
    assert len(frontier.rows) == 4


# 7
def test_a_seeded_task_id_depends_only_on_its_target():
    # Each occurrence is its own frontier row, and success enqueues the next
    # one. Anchoring the first occurrence at a fixed instant is what keeps a
    # re-seed from starting a second chain for a target already being crawled.
    frontier = RecordingFrontier()
    _seed(frontier, categories=["1846"], pages=1)

    (task,) = frontier.rows.values()
    assert task.task_id == make_crawl_task_id("tiki", ResourceType.LISTING_PAGE, task.target, seed_frontier.SEED_SCHEDULED_FOR)


def test_seeding_refuses_an_empty_or_malformed_universe():
    with pytest.raises(ValueError, match="categories"):
        _seed(RecordingFrontier(), categories=[])
    with pytest.raises(ValueError, match="categories"):
        _seed(RecordingFrontier(), categories=["  "])
    with pytest.raises(ValueError, match="pages"):
        _seed(RecordingFrontier(), pages=0)


# ----------------------------------------------------------------------------
# The default suite stays offline.
# ----------------------------------------------------------------------------
def test_importing_the_service_and_seeder_opens_no_client():
    probe = (
        "import crawler.service, crawler.seed_frontier;"
        "import sys;"
        "assert 'psycopg2' not in sys.modules, 'psycopg2';"
        "assert 'kafka' not in sys.modules, 'kafka';"
        "assert 'minio' not in sys.modules, 'minio';"
        "assert 'requests' not in sys.modules, 'requests';"
        "print('CLEAN')"
    )
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=300)

    assert "CLEAN" in result.stdout, result.stderr


def test_both_entrypoints_answer_help():
    for module in ("crawler.service", "crawler.seed_frontier"):
        result = subprocess.run([sys.executable, "-m", module, "--help"], capture_output=True, text=True, timeout=300)
        assert result.returncode == 0, result.stderr
        assert "usage" in result.stdout.lower()


# ----------------------------------------------------------------------------
# The PostgreSQL factory closes what it opens.
# ----------------------------------------------------------------------------
class FakeConnection:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        self.log.append("begin")
        return self

    def __exit__(self, kind, value, traceback):
        self.log.append("rollback" if kind else "commit")
        return False

    def close(self):
        self.log.append("close")


def _fake_psycopg2(monkeypatch, log):
    module = type(sys)("psycopg2")
    module.connect = lambda **kwargs: log.append("connect") or FakeConnection(log)
    monkeypatch.setitem(sys.modules, "psycopg2", module)


def test_the_connection_factory_commits_and_closes_each_connection(monkeypatch):
    # with psycopg2's own connection, "with conn" commits but never closes,
    # so a long-running service would leak one connection per call.
    log = []
    _fake_psycopg2(monkeypatch, log)
    factory = service.postgres_connection_factory()

    with factory() as conn:
        assert isinstance(conn, FakeConnection)

    assert log == ["connect", "begin", "commit", "close"]


def test_the_connection_factory_rolls_back_and_still_closes_on_error(monkeypatch):
    log = []
    _fake_psycopg2(monkeypatch, log)
    factory = service.postgres_connection_factory()

    with pytest.raises(RuntimeError):
        with factory():
            raise RuntimeError("statement failed")

    assert log == ["connect", "begin", "rollback", "close"]

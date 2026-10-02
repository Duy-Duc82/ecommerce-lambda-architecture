"""Phase 8 plan section 6.5: one marketplace batch at a time (tests 17-18)."""
from __future__ import annotations

import json
import sys
from contextlib import contextmanager

import pytest

from batch_layer import marketplace_warehouse as warehouse
from batch_layer.marketplace_lock import ALREADY_RUNNING_EXIT_CODE, BatchAlreadyRunning, exclusive_batch
from tests.test_marketplace_warehouse import batch_context, empty_reader, noop_writer, wire_orchestration


class FakeServer:
    """Advisory locks as PostgreSQL keeps them: per key, owned by a session."""

    def __init__(self):
        self.held: dict[int, object] = {}
        self.statements: list[str] = []
        self.closed = 0


class FakeCursor:
    def __init__(self, server, session):
        self.server, self.session, self.result = server, session, None

    def __enter__(self): return self
    def __exit__(self, *exc): return False

    def execute(self, sql, params):
        (key,) = params
        self.server.statements.append(sql.split("(")[0].split()[-1])
        if "pg_try_advisory_lock" in sql:
            owner = self.server.held.setdefault(key, self.session)
            self.result = (owner is self.session,)
        elif "pg_advisory_unlock" in sql:
            self.result = (self.server.held.pop(key, None) is self.session,)

    def fetchone(self): return self.result


class FakeConnection:
    def __init__(self, server):
        self.server = server

    def cursor(self): return FakeCursor(self.server, self)
    def commit(self): pass


def factory_for(server):
    @contextmanager
    def connect():
        try:
            yield FakeConnection(server)
        finally:
            server.closed += 1
    return connect


# 17
def test_a_held_lock_refuses_the_second_batch():
    server = FakeServer()
    factory = factory_for(server)

    with exclusive_batch(factory, key=7):
        with pytest.raises(BatchAlreadyRunning) as refused:
            with exclusive_batch(factory, key=7):
                pytest.fail("the body must not run without the lock")
    assert refused.value.key == 7
    # The refused attempt never asked to unlock what it never held.
    assert server.statements == ["pg_try_advisory_lock", "pg_try_advisory_lock", "pg_advisory_unlock"]


# 17
def test_the_lock_is_released_on_exit_and_on_error():
    server = FakeServer()
    factory = factory_for(server)

    with exclusive_batch(factory, key=7):
        assert 7 in server.held
    assert server.held == {}

    with pytest.raises(RuntimeError, match="spark died"):
        with exclusive_batch(factory, key=7):
            raise RuntimeError("spark died")
    assert server.held == {}
    # Every dedicated connection was closed, which alone releases a session lock.
    assert server.closed == 2


# 17
def test_a_failed_unlock_does_not_hide_the_run_error():
    class LostCursor(FakeCursor):
        def execute(self, sql, params):
            if "pg_advisory_unlock" in sql:
                raise ConnectionError("connection lost")
            super().execute(sql, params)

    class LostConnection(FakeConnection):
        def cursor(self): return LostCursor(self.server, self)

    server = FakeServer()

    @contextmanager
    def connect():
        yield LostConnection(server)

    with pytest.raises(RuntimeError, match="spark died"):
        with exclusive_batch(connect, key=7):
            raise RuntimeError("spark died")


# 18
def test_a_run_refused_by_the_lock_starts_nothing(monkeypatch):
    built = []
    parts = wire_orchestration(monkeypatch, lock_refused=True)
    monkeypatch.setattr(warehouse, "build_spark", lambda: built.append("spark"))

    with pytest.raises(BatchAlreadyRunning):
        warehouse.run_marketplace_warehouse(batch_context(), writer=noop_writer, reader=empty_reader)

    # The fake audit logs here too: no start_run, so no audit row.
    assert parts["log"] == []
    assert built == []


# 18
def test_the_lock_covers_a_run_without_postgres(monkeypatch):
    parts = wire_orchestration(monkeypatch)

    warehouse.run_marketplace_warehouse(batch_context(), publish_cache=False, writer=noop_writer, reader=empty_reader)

    assert parts["lock"] == ["acquire", "release"]


# 18
def test_the_cli_reports_already_running_and_exits_75(monkeypatch, capsys):
    def refused(context, **kwargs):
        raise BatchAlreadyRunning(820801)

    monkeypatch.setattr(warehouse, "run_marketplace_warehouse", refused)
    monkeypatch.setattr(sys, "argv", ["marketplace_warehouse", "--run-id", "run-1", "--as-of", "2026-09-05T10:00:00Z"])

    with pytest.raises(SystemExit) as exited:
        warehouse.main()

    assert exited.value.code == ALREADY_RUNNING_EXIT_CODE == 75
    report = json.loads(capsys.readouterr().out)
    assert report == {"run_id": "run-1", "status": "ALREADY_RUNNING", "lock_key": 820801}

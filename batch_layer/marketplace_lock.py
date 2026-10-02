"""One marketplace batch at a time — Phase 8 plan section 6.5.

Two runs writing the same Gold root and racing for the ``current`` pointer
would publish whichever finished last, not whichever window is newest. A
PostgreSQL session advisory lock rules that out without a new service: it is
held on a connection of its own for the whole run, and PostgreSQL releases it
by itself if the process dies and the connection drops.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Iterator

from config.settings import MARKETPLACE_BATCH_LOCK_KEY

# EX_TEMPFAIL: try again later. The scheduler's next tick is that later.
ALREADY_RUNNING_EXIT_CODE = 75


class BatchAlreadyRunning(RuntimeError):
    def __init__(self, key: int):
        super().__init__(f"another marketplace batch holds advisory lock {key}")
        self.key = key


@contextmanager
def exclusive_batch(connection_factory: Callable[[], Any], *, key: int = MARKETPLACE_BATCH_LOCK_KEY) -> Iterator[None]:
    """Hold the batch lock for the body, or raise ``BatchAlreadyRunning``.

    ``connection_factory`` must hand out a dedicated connection: the lock
    belongs to the session, so sharing that connection would share the lock.
    """
    with connection_factory() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
            acquired = bool(cur.fetchone()[0])
        # A session lock outlives the transaction. Ending the transaction now
        # keeps this connection from idling in one for the whole run, which
        # would hold back vacuum on every table for as long.
        conn.commit()
        if not acquired:
            raise BatchAlreadyRunning(key)
        try:
            yield
        finally:
            # A failed unlock must not hide the run's own error. The factory
            # closes the connection next, and closing it releases the lock.
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
                conn.commit()
            except Exception:
                pass

"""PostgreSQL connections for long-running services.

The repositories in this codebase open connections as
``with factory() as conn, conn.cursor() as cur``. On a bare psycopg2
connection that block commits or rolls back but never closes the
connection, which a service running for days would leak once per call until
PostgreSQL ran out of connections. The factory here closes what it opens.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable


def postgres_connection_factory() -> Callable[[], Any]:
    import psycopg2

    from config.settings import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER

    @contextmanager
    def connect():
        conn = psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER,
                                password=POSTGRES_PASSWORD, dbname=POSTGRES_DB)
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    return connect

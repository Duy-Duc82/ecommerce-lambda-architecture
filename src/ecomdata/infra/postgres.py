"""PostgreSQL access: the one connection factory and the one JDBC URL.

Repositories open connections as ``with factory() as conn, conn.cursor() as
cur``. On a bare psycopg2 connection that block commits or rolls back but
never closes, which a service running for days would leak once per call; the
factory here closes what it opens.
"""
from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, Protocol


class PostgresTarget(Protocol):
    """The shape of ``ecomdata.settings.PostgresSettings``."""

    host: str
    port: int
    user: str
    password: str
    db: str


def connection_factory(pg: PostgresTarget) -> Callable[[], Any]:
    """A zero-argument callable returning a context manager over one connection."""

    @contextmanager
    def connect():
        import psycopg2

        conn = psycopg2.connect(host=pg.host, port=pg.port, user=pg.user, password=pg.password, dbname=pg.db)
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    return connect


def jdbc_url(pg: PostgresTarget) -> str:
    return f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.db}"


def jdbc_properties(pg: PostgresTarget) -> dict[str, str]:
    return {"user": pg.user, "password": pg.password, "driver": "org.postgresql.Driver"}


def libpq_env(pg: PostgresTarget) -> dict[str, str]:
    """Environment for ``pg_dump``/``pg_restore``/``psql``."""
    return {"PGHOST": pg.host, "PGPORT": str(pg.port), "PGUSER": pg.user, "PGPASSWORD": pg.password}

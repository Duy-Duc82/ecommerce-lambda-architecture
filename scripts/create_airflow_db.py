"""Create the ``airflow`` database on postgres-dw if it is missing (idempotent).

Run by the airflow-init container before ``airflow db migrate``. A fresh
postgres volume could create it from an init script, but an existing volume
never re-runs those, so the check lives here.
"""
from __future__ import annotations

import os

import psycopg2

DATABASE = "airflow"


def main() -> None:
    connection = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres-dw"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ.get("POSTGRES_DB", "data_warehouse"),
    )
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE,))
            if cursor.fetchone() is None:
                cursor.execute(f'CREATE DATABASE "{DATABASE}"')
                print(f"created database {DATABASE}")
            else:
                print(f"database {DATABASE} exists")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

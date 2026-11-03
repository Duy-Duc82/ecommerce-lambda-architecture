"""Composition root of the operator tools.

The tools in ``ecomdata.ops`` take their configuration as arguments; the
``main()`` of a tool and ``ecomdata.ops.cli`` get it from here. Settings are
read once per process.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from ecomdata.settings import Settings, load_settings


@lru_cache(maxsize=1)
def settings() -> Settings:
    return load_settings(dotenv=True)


def object_store(config: Settings | None = None) -> Any:
    from ecomdata.infra.object_store import object_store_for

    return object_store_for((config or settings()).storage)


def connection_factory(config: Settings | None = None) -> Any:
    from ecomdata.infra.postgres import connection_factory as factory

    return factory((config or settings()).postgres)


def producer(config: Settings | None = None) -> Any:
    from ecomdata.infra.kafka import create_producer

    return create_producer((config or settings()).kafka.bootstrap_servers)

"""Composition root of the serving tools (Kibana/Superset provisioning, ES projector)."""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from ecomdata.settings import Settings, load_settings


@lru_cache(maxsize=1)
def settings() -> Settings:
    return load_settings(dotenv=True)


def connection_factory(config: Settings | None = None) -> Any:
    from ecomdata.infra.postgres import connection_factory as factory

    return factory((config or settings()).postgres)

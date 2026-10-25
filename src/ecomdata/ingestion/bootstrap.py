"""Composition root of the ingestion tools: settings and the lake, read once per process."""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from ecomdata.settings import Settings, load_settings


@lru_cache(maxsize=1)
def settings() -> Settings:
    return load_settings(dotenv=True)


def object_store() -> Any:
    from ecomdata.infra.object_store import object_store_for

    return object_store_for(settings().storage)

"""Composition root of the serving tools (Kibana/Superset provisioning, ES projector)."""
from __future__ import annotations

from functools import lru_cache

from ecomdata.settings import Settings, load_settings


@lru_cache(maxsize=1)
def settings() -> Settings:
    return load_settings(dotenv=True)

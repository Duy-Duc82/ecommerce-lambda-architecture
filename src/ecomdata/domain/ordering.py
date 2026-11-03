"""The one ordering of an offer's observations, and its tie-break.

Observations of one offer are ordered by when they were observed, then when
the response that carried them was fetched, then by ``observation_id`` so two
rows equal on both instants still have a fixed order. The batch marts build
their Spark windows from :data:`ORDER_COLUMNS`; the speed layer sorts and
judges lateness with :func:`order_key`. Before this module the speed layer
used ``(observed_at, observation_id)`` and the batch layer the three columns
below, so a tie on ``observed_at`` could resolve differently in each.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

ORDER_COLUMNS: tuple[str, str, str] = ("observed_at", "fetched_at", "observation_id")


def order_key(observed_at: datetime, fetched_at: datetime | None, observation_id: str) -> tuple[Any, ...]:
    """Sort key for one observation; a missing ``fetched_at`` sorts as ``observed_at``."""
    return (observed_at, fetched_at if fetched_at is not None else observed_at, observation_id)


def is_before(left: tuple[Any, ...], right: tuple[Any, ...]) -> bool:
    return left < right

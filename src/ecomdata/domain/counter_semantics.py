"""Which public counters may have their deltas trusted, per source.

The answer lives on each :class:`~ecomdata.domain.sources.SourceSpec`; this
module only looks it up, so a source unknown to the registry trusts nothing.
"""
from __future__ import annotations

from ecomdata.domain.sources import SOURCES, CounterSemantic, SourceRegistry

__all__ = ["CounterSemantic", "counter_semantic", "counter_semantics_for"]


def counter_semantics_for(marketplace: str, registry: SourceRegistry = SOURCES) -> tuple[CounterSemantic, ...]:
    spec = registry.find(str(marketplace).strip().lower())
    return spec.counter_semantics() if spec is not None else ()


def counter_semantic(marketplace: str, field_name: str, registry: SourceRegistry = SOURCES) -> CounterSemantic | None:
    return next((item for item in counter_semantics_for(marketplace, registry) if item.field_name == field_name), None)

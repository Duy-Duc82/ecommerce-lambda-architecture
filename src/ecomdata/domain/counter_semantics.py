"""Explicit registry for counters whose deltas may be trusted."""
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class CounterSemantic:
    marketplace: str
    field_name: str
    enabled: bool
    monotonic_expected: bool
    semantic_version: str


_DEFAULT = {
    "tiki": ("v1", True),
    "yame": ("v1", True),
    "emwear": ("v1", True),
    "allbirds": ("v1", True),
}
_FIELDS = ("rating_count", "review_count", "sold_count")


def counter_semantics_for(marketplace: str) -> tuple[CounterSemantic, ...]:
    code = marketplace.strip().lower()
    if code not in _DEFAULT: return ()
    version, monotonic = _DEFAULT[code]
    return tuple(CounterSemantic(code, field, True, monotonic, version) for field in _FIELDS)


def counter_semantic(marketplace: str, field_name: str) -> CounterSemantic | None:
    return next((x for x in counter_semantics_for(marketplace) if x.field_name == field_name), None)

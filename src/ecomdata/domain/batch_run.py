"""The lifecycle of one marketplace batch run, as an explicit state machine.

::

    CREATED -> SILVER_BUILT -> DIMENSIONS_BUILT -> GOLD_WRITTEN -> QUALITY_PASSED -> PUBLISHED -> PROMOTED
                                                                +-> QUALITY_FAILED
    any non-terminal state -> FAILED;  QUALITY_PASSED -> HELD  (kept off the serving version)

The cache is published before the ``current`` pointer moves, as it was before
stages existed: a pointer must never name a Gold run whose cache was not
written. A run is PROMOTED only once the pointer names it.

A FAILED run is resumed from the state it reached last, which the audit row
keeps in ``last_completed_status``; a HELD run resumes at publication once an
operator allows it (``--allow-backfill``).
"""
from __future__ import annotations

from enum import Enum


class BatchRunStatus(str, Enum):
    CREATED = "CREATED"
    SILVER_BUILT = "SILVER_BUILT"
    # The star's dimensions are merged into the lake's dw/ tables (Phase 10).
    DIMENSIONS_BUILT = "DIMENSIONS_BUILT"
    GOLD_WRITTEN = "GOLD_WRITTEN"
    QUALITY_PASSED = "QUALITY_PASSED"
    QUALITY_FAILED = "QUALITY_FAILED"
    PUBLISHED = "PUBLISHED"
    PROMOTED = "PROMOTED"
    HELD = "HELD"
    FAILED = "FAILED"


S = BatchRunStatus

TRANSITIONS: dict[BatchRunStatus, frozenset[BatchRunStatus]] = {
    S.CREATED: frozenset({S.SILVER_BUILT, S.FAILED}),
    S.SILVER_BUILT: frozenset({S.DIMENSIONS_BUILT, S.FAILED}),
    S.DIMENSIONS_BUILT: frozenset({S.GOLD_WRITTEN, S.FAILED}),
    S.GOLD_WRITTEN: frozenset({S.QUALITY_PASSED, S.QUALITY_FAILED, S.FAILED}),
    S.QUALITY_PASSED: frozenset({S.PUBLISHED, S.HELD, S.FAILED}),
    S.PUBLISHED: frozenset({S.PROMOTED, S.HELD, S.FAILED}),
    S.HELD: frozenset({S.PUBLISHED, S.PROMOTED, S.FAILED}),
    S.QUALITY_FAILED: frozenset(),
    S.PROMOTED: frozenset(),
    S.FAILED: frozenset(),
}

TERMINAL = frozenset(status for status, targets in TRANSITIONS.items() if not targets)
# A window whose run reached one of these is done: a scheduler skips it.
COMPLETED = frozenset({S.PROMOTED})

# Status names a pre-stage audit row may still hold, and what they mean now.
LEGACY_STATUS = {"RUNNING": S.CREATED, "SUCCEEDED": S.PROMOTED}


class InvalidTransition(ValueError):
    def __init__(self, current: BatchRunStatus, target: BatchRunStatus):
        super().__init__(f"batch run cannot go from {current.value} to {target.value}")
        self.current, self.target = current, target


def parse_status(value: str | BatchRunStatus) -> BatchRunStatus:
    if isinstance(value, BatchRunStatus):
        return value
    if value in LEGACY_STATUS:
        return LEGACY_STATUS[value]
    return BatchRunStatus(value)


def can_transition(current: BatchRunStatus, target: BatchRunStatus) -> bool:
    return target in TRANSITIONS[current]


def check_transition(current: BatchRunStatus, target: BatchRunStatus) -> None:
    if not can_transition(current, target):
        raise InvalidTransition(current, target)


# The order a full run passes through, and how far each status is along it.
PROGRESS = (S.CREATED, S.SILVER_BUILT, S.DIMENSIONS_BUILT, S.GOLD_WRITTEN, S.QUALITY_PASSED, S.PUBLISHED, S.PROMOTED)


def reached(current: BatchRunStatus, milestone: BatchRunStatus) -> bool:
    """Whether a run in ``current`` has already passed ``milestone`` on the happy path."""
    if current not in PROGRESS or milestone not in PROGRESS:
        return False
    return PROGRESS.index(current) >= PROGRESS.index(milestone)

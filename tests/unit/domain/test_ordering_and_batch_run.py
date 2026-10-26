"""The shared observation ordering and the batch-run state machine."""
from datetime import UTC, datetime, timedelta

import pytest

from ecomdata.domain.batch_run import (
    COMPLETED,
    TERMINAL,
    TRANSITIONS,
    BatchRunStatus,
    InvalidTransition,
    can_transition,
    check_transition,
    parse_status,
    reached,
)
from ecomdata.domain.ordering import ORDER_COLUMNS, order_key

S = BatchRunStatus
T0 = datetime(2026, 10, 24, tzinfo=UTC)


def test_order_is_observed_then_fetched_then_id():
    assert ORDER_COLUMNS == ("observed_at", "fetched_at", "observation_id")
    rows = [
        order_key(T0, T0 + timedelta(seconds=2), "obs-a"),
        order_key(T0, T0 + timedelta(seconds=1), "obs-z"),
        order_key(T0, T0 + timedelta(seconds=1), "obs-b"),
        order_key(T0 - timedelta(seconds=1), T0 + timedelta(days=1), "obs-y"),
    ]
    assert [key[2] for key in sorted(rows)] == ["obs-y", "obs-b", "obs-z", "obs-a"]


def test_missing_fetched_at_sorts_as_observed_at():
    assert order_key(T0, None, "x") == (T0, T0, "x")


HAPPY = [S.CREATED, S.SILVER_BUILT, S.DIMENSIONS_BUILT, S.FACTS_BUILT, S.GOLD_WRITTEN, S.QUALITY_PASSED, S.PUBLISHED, S.PROMOTED]


def test_the_happy_path_is_a_chain_of_valid_transitions():
    for current, target in zip(HAPPY, HAPPY[1:], strict=False):
        check_transition(current, target)


@pytest.mark.parametrize("current, target", [(c, t) for c in S for t in S if t not in TRANSITIONS[c]])
def test_every_transition_outside_the_table_is_refused(current, target):
    assert not can_transition(current, target)
    with pytest.raises(InvalidTransition, match=f"from {current.value} to {target.value}"):
        check_transition(current, target)


@pytest.mark.parametrize("current, target", [(c, t) for c in S for t in TRANSITIONS[c]])
def test_every_transition_in_the_table_is_allowed(current, target):
    check_transition(current, target)


def test_terminal_states_and_completion():
    assert TERMINAL == {S.QUALITY_FAILED, S.PROMOTED, S.FAILED}
    assert COMPLETED == {S.PROMOTED}
    for status in S:
        if status not in TERMINAL:
            assert can_transition(status, S.FAILED)


def test_publication_comes_before_promotion():
    assert can_transition(S.QUALITY_PASSED, S.PUBLISHED)
    assert not can_transition(S.QUALITY_PASSED, S.PROMOTED)
    assert can_transition(S.PUBLISHED, S.PROMOTED)


def test_legacy_audit_statuses_are_read():
    assert parse_status("SUCCEEDED") is S.PROMOTED
    assert parse_status("RUNNING") is S.CREATED
    assert parse_status("HELD") is S.HELD
    with pytest.raises(ValueError):
        parse_status("DONE")


def test_reached_follows_the_happy_path():
    assert reached(S.GOLD_WRITTEN, S.SILVER_BUILT)
    assert not reached(S.SILVER_BUILT, S.GOLD_WRITTEN)
    assert not reached(S.HELD, S.PUBLISHED)

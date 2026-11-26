"""The counter gap against the crawl cadences (run of 2026-11-07, item 4).

A target in the slowest tier is seen once per cadence, plus the hours a sweep
takes. With MARKETPLACE_COUNTER_MAX_GAP_SECONDS at or under that cadence, every
counter transition of the tier is GAP_TOO_LONG by construction and the counter
marts hold no valid rate for it, while nothing looks broken.
"""
from __future__ import annotations

import pytest

from ecomdata.settings import SettingsError, load_settings


def test_the_default_gap_tolerates_one_missed_sweep_and_not_two():
    settings = load_settings({})

    sweep_seconds = settings.crawl.cadence_minutes["SWEEP"] * 60
    assert settings.batch.counter_max_gap_seconds == 2 * sweep_seconds == 172800


def test_a_gap_under_one_and_a_half_times_the_slowest_cadence_is_refused():
    with pytest.raises(SettingsError, match="MARKETPLACE_COUNTER_MAX_GAP_SECONDS.*CRAWL_SWEEP_CADENCE_MINUTES=1440"):
        load_settings({"MARKETPLACE_COUNTER_MAX_GAP_SECONDS": "86400"})

    # Exactly 1.5 times passes; the slowest tier is whichever is configured slowest.
    load_settings({"MARKETPLACE_COUNTER_MAX_GAP_SECONDS": str(1440 * 60 * 3 // 2)})
    with pytest.raises(SettingsError, match="CRAWL_COLD_CADENCE_MINUTES=4320"):
        load_settings({"CRAWL_COLD_CADENCE_MINUTES": "4320"})


def test_a_slower_sweep_needs_a_wider_gap():
    env = {"CRAWL_SWEEP_CADENCE_MINUTES": "2880"}

    with pytest.raises(SettingsError, match="MARKETPLACE_COUNTER_MAX_GAP_SECONDS"):
        load_settings(env)

    assert load_settings({**env, "MARKETPLACE_COUNTER_MAX_GAP_SECONDS": "345600"}).batch.counter_max_gap_seconds == 345600

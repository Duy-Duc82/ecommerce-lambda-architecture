"""The replayed-fixture load generator — Phase 9 plan section 7 (tests 3-4)."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from config.marketplace_schema import make_observation_id
from config.marketplace_wire import canonical_json, marketplace_observation_from_wire
from ops import bench_load
from ops.bench_load import LoadSpec

START = datetime(2026, 10, 4, 13, 0, tzinfo=timezone.utc)
SENT_AT = datetime(2026, 10, 4, 13, 5, tzinfo=timezone.utc)


def _events(spec):
    return list(bench_load.fixture_events(spec, clock=lambda: SENT_AT))


def test_one_seed_and_start_send_byte_identical_events():
    spec = LoadSpec(seed=7, start=START, count=50, offers=10)

    assert [canonical_json(e) for e in _events(spec)] == [canonical_json(e) for e in _events(spec)]


def test_another_seed_sends_other_offers():
    first = {e.payload.offer.offer_id for e in _events(LoadSpec(seed=1, start=START, count=10, offers=10))}
    second = {e.payload.offer.offer_id for e in _events(LoadSpec(seed=2, start=START, count=10, offers=10))}

    assert first.isdisjoint(second)


def test_every_event_passes_the_wire_contract_and_the_frozen_id_derivation():
    for event in _events(LoadSpec(seed=3, start=START, count=30, offers=7)):
        wire = json.loads(canonical_json(event))
        assert marketplace_observation_from_wire(wire) == event
        obs = event.payload.observation
        assert obs.observation_id == make_observation_id(
            "tiki", event.payload.offer.platform_listing_id, obs.observed_at, obs.raw_sha256)
        assert event.partition_key == f"tiki:{event.payload.offer.platform_listing_id}"
        assert event.produced_at == SENT_AT


def test_rounds_revisit_each_offer_with_the_stub_s_price_table():
    events = _events(LoadSpec(seed=4, start=START, count=8, offers=2))
    by_offer = {}
    for event in events:
        by_offer.setdefault(event.payload.offer.offer_id, []).append(event)

    assert len(by_offer) == 2
    for observations in by_offer.values():
        prices = [e.payload.observation.current_price for e in observations]
        base = prices[0]
        assert prices == [base * p // 100 for p in bench_load.PRICE_STEPS]
        instants = [e.payload.observation.observed_at for e in observations]
        assert instants == sorted(instants) and len(set(instants)) == 4


def test_the_duplicate_ratio_re_sends_exactly_the_hashed_events_right_after_themselves():
    spec = LoadSpec(seed=5, start=START, count=400, offers=50, duplicate_ratio=0.25)
    events = _events(spec)
    expected = [i for i in range(spec.count) if bench_load.is_duplicate(spec, i)]

    assert len(events) == spec.count + len(expected)
    assert 60 <= len(expected) <= 140          # about a quarter, by a fixed hash
    ids = [e.event_id for e in events]
    repeated = [ids[k] for k in range(1, len(ids)) if ids[k] == ids[k - 1]]
    assert len(repeated) == len(expected)


def test_no_duplicates_without_a_ratio():
    events = _events(LoadSpec(seed=6, start=START, count=200, offers=20))

    assert len({e.event_id for e in events}) == 200


@pytest.mark.parametrize("bad", [dict(count=-1), dict(offers=0), dict(duplicate_ratio=1.0),
                                 dict(start=datetime(2026, 10, 4))])
def test_a_spec_out_of_range_is_refused(bad):
    values = dict(seed=1, start=START, count=1)
    values.update(bad)
    with pytest.raises(ValueError):
        LoadSpec(**values)


def test_the_pacer_holds_the_rate_without_sleeping_ahead_of_time():
    now = [0.0]
    slept = []

    def sleep(seconds):
        slept.append(round(seconds, 6))
        now[0] += seconds

    pacer = bench_load.Pacer(10, monotonic=lambda: now[0], sleep=sleep)
    for _ in range(4):
        pacer.wait()

    assert slept == [0.1, 0.1, 0.1]
    assert bench_load.Pacer(0, monotonic=lambda: 0.0, sleep=lambda s: pytest.fail("unpaced")).wait() is None


def test_send_keys_each_record_as_the_crawler_does_and_waits_for_every_ack():
    class Future:
        def __init__(self):
            self.got = False

        def get(self, timeout):
            self.got = True

    class Producer:
        def __init__(self):
            self.sent, self.flushes = [], 0

        def send(self, topic, key, value):
            future = Future()
            self.sent.append((topic, key, value, future))
            return future

        def flush(self):
            self.flushes += 1

    producer = Producer()
    events = _events(LoadSpec(seed=8, start=START, count=5, offers=5))

    assert bench_load.send(events, producer, bench_load.Pacer(0), flush_every=2) == 5
    assert [s[0] for s in producer.sent] == ["marketplace.observations.v1"] * 5
    assert [s[1] for s in producer.sent] == [e.partition_key for e in events]
    assert all(s[3].got for s in producer.sent)


def test_the_generator_refuses_the_live_project(monkeypatch):
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "mp-live")

    with pytest.raises(SystemExit):
        bench_load.main(["--count", "1"])

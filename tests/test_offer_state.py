"""Phase 5 SPD-03: offer state store and Redis serving writes."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from config.marketplace_schema import Availability, MarketplaceChangeType, create_change_event
from serving_layer.marketplace_redis import FORBIDDEN_COMMANDS, MarketplaceRedisWriter
from speed_layer.change_rules import OfferStateSnapshot
from speed_layer.offer_state import (
    BackwardsStateWrite,
    InMemoryOfferStateStore,
    RedisOfferStateStore,
    state_from_mapping,
    state_to_mapping,
)

T0 = datetime(2026, 9, 1, 3, 0, tzinfo=timezone.utc)


def _state(offer_id="offer_1", *, observed_at=T0, observation_id="obs_1", price="1000000"):
    return OfferStateSnapshot(
        offer_id=offer_id,
        marketplace="tiki",
        platform_listing_id="279212151",
        observation_id=observation_id,
        observed_at=observed_at,
        current_price=Decimal(price),
        list_price=Decimal("1200000"),
        availability=Availability.IN_STOCK,
        rating_value=Decimal("4.5"),
        review_count=10,
        sold_count=100,
        raw_uri="s3a://ecommerce-bronze/marketplace/raw/body.bin",
    )


class FakeRedis:
    """Minimal Redis double recording every command it is asked to run."""

    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.expiries: dict[str, int] = {}
        self.commands: list[str] = []
        self.pipelines = 0

    # --- direct commands ---
    def hset(self, key, mapping=None, **kwargs):
        self.commands.append("hset")
        self.hashes.setdefault(key, {}).update(mapping or {})

    def hgetall(self, key):
        self.commands.append("hgetall")
        return dict(self.hashes.get(key, {}))

    def sadd(self, key, *members):
        self.commands.append("sadd")
        self.sets.setdefault(key, set()).update(members)

    def smembers(self, key):
        self.commands.append("smembers")
        return set(self.sets.get(key, set()))

    def zadd(self, key, mapping):
        self.commands.append("zadd")
        self.zsets.setdefault(key, {}).update(mapping)

    def zremrangebyrank(self, key, start, stop):
        self.commands.append("zremrangebyrank")
        entries = sorted(self.zsets.get(key, {}).items(), key=lambda kv: kv[1])
        if stop < 0:
            stop = len(entries) + stop
        for member, _ in entries[start : stop + 1]:
            self.zsets[key].pop(member, None)

    def expire(self, key, seconds):
        self.commands.append("expire")
        self.expiries[key] = seconds

    def pipeline(self):
        self.pipelines += 1
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis: FakeRedis):
        self._redis = redis
        self._queued: list[tuple[str, tuple, dict]] = []

    def __getattr__(self, name):
        def queue(*args, **kwargs):
            self._queued.append((name, args, kwargs))
            return self

        return queue

    def execute(self):
        results = []
        for name, args, kwargs in self._queued:
            results.append(getattr(self._redis, name)(*args, **kwargs))
        self._queued.clear()
        return results


@pytest.fixture(params=["memory", "redis"])
def store(request):
    if request.param == "memory":
        return InMemoryOfferStateStore()
    return RedisOfferStateStore(FakeRedis(), namespace="rt")


# --- 22: one shared body against both implementations ----------------------


def test_put_then_get_round_trips(store):
    state = _state()
    store.put_many([state])
    assert store.get_many(["offer_1"])["offer_1"] == state


def test_get_many_ignores_unknown_offers(store):
    store.put_many([_state("offer_1")])
    assert set(store.get_many(["offer_1", "offer_missing"])) == {"offer_1"}


def test_get_many_of_nothing_is_empty(store):
    assert store.get_many([]) == {}


def test_forward_write_is_accepted(store):
    store.put_many([_state(observed_at=T0)])
    store.put_many([_state(observed_at=T0 + timedelta(hours=1), observation_id="obs_2")])
    assert store.get_many(["offer_1"])["offer_1"].observation_id == "obs_2"


# --- 24: backwards write refused -------------------------------------------


def test_backwards_write_is_refused(store):
    store.put_many([_state(observed_at=T0 + timedelta(hours=5))])
    with pytest.raises(BackwardsStateWrite):
        store.put_many([_state(observed_at=T0, observation_id="obs_old")])
    assert store.get_many(["offer_1"])["offer_1"].observed_at == T0 + timedelta(hours=5)


# --- 25: bounded, deterministic sweep scan ---------------------------------


def test_scan_states_is_bounded_and_ordered(store):
    store.put_many(
        [
            _state("offer_c", observed_at=T0 + timedelta(minutes=30), observation_id="o3"),
            _state("offer_a", observed_at=T0 + timedelta(minutes=10), observation_id="o1"),
            _state("offer_b", observed_at=T0 + timedelta(minutes=20), observation_id="o2"),
        ]
    )
    scanned = store.scan_states(2)
    assert [state.offer_id for state in scanned] == ["offer_a", "offer_b"]
    assert store.scan_states(2) == scanned


def test_scan_states_rejects_negative_limit(store):
    with pytest.raises(ValueError):
        store.scan_states(-1)


# --- 23, 26: Redis-specific behaviour --------------------------------------


def test_get_many_issues_one_pipeline():
    client = FakeRedis()
    redis_store = RedisOfferStateStore(client, namespace="rt")
    redis_store.get_many([f"offer_{index}" for index in range(50)])
    assert client.pipelines == 1
    assert client.commands.count("hgetall") == 50


def test_state_store_issues_no_arithmetic_command():
    client = FakeRedis()
    redis_store = RedisOfferStateStore(client, namespace="rt")
    redis_store.put_many([_state()])
    redis_store.get_many(["offer_1"])
    redis_store.scan_states(10)
    assert not set(client.commands) & set(FORBIDDEN_COMMANDS)


def test_redis_state_mapping_keeps_money_as_exact_string():
    mapping = state_to_mapping(_state(price="16990000.500000"))
    assert mapping["current_price"] == "16990000.500000"
    assert state_from_mapping(mapping).current_price == Decimal("16990000.500000")


def test_state_mapping_round_trips_optional_nulls():
    state = OfferStateSnapshot(
        offer_id="offer_1",
        marketplace="tiki",
        platform_listing_id="1",
        observation_id="obs_1",
        observed_at=T0,
        current_price=Decimal("10"),
        list_price=None,
        availability=Availability.UNKNOWN,
        rating_value=None,
        review_count=None,
        sold_count=None,
        raw_uri="s3a://b/k",
    )
    assert state_from_mapping(state_to_mapping(state)) == state


def test_stored_naive_observed_at_is_rejected():
    mapping = state_to_mapping(_state())
    mapping["observed_at"] = "2026-09-01T03:00:00"
    with pytest.raises(ValueError, match="timezone-aware"):
        state_from_mapping(mapping)


def test_redis_store_requires_namespace():
    with pytest.raises(ValueError):
        RedisOfferStateStore(FakeRedis(), namespace="  ")


# --- 27: recent-change trimming and serving writes -------------------------


def _change(index: int, *, detected_at=T0):
    return create_change_event(
        marketplace_code="tiki",
        offer_id=f"offer_{index}",
        change_type=MarketplaceChangeType.PRICE_CHANGED,
        current_observation_id=f"obs_{index}",
        detected_at=detected_at,
        rule_version="marketplace-change-rules.v1",
        previous_observation_id=f"obs_prev_{index}",
        field_name="current_price",
        previous_value=Decimal("10"),
        current_value=Decimal("9"),
    )


def _writer(client, *, recent_max=3):
    return MarketplaceRedisWriter(
        client,
        namespace="rt",
        recent_changes_max=recent_max,
        change_doc_ttl_seconds=604800,
        freshness_threshold_minutes=360,
    )


def test_recent_changes_are_trimmed_to_the_configured_maximum():
    client = FakeRedis()
    writer = _writer(client, recent_max=3)
    changes = [_change(index, detected_at=T0 + timedelta(minutes=index)) for index in range(6)]
    writer.write_changes(changes, {change.event_id: {"marketplace": "tiki"} for change in changes})
    assert len(client.zsets["rt:changes:tiki"]) == 3
    newest = {change.event_id for change in changes[-3:]}
    assert set(client.zsets["rt:changes:tiki"]) == newest


def test_replaying_a_change_overwrites_instead_of_appending():
    client = FakeRedis()
    writer = _writer(client)
    change = _change(1)
    documents = {change.event_id: {"marketplace": "tiki"}}
    writer.write_changes([change], documents)
    writer.write_changes([change], documents)
    assert list(client.zsets["rt:changes:tiki"]) == [change.event_id]


def test_change_documents_get_the_configured_ttl():
    client = FakeRedis()
    writer = _writer(client)
    change = _change(1)
    writer.write_changes([change], {change.event_id: {"marketplace": "tiki"}})
    assert client.expiries[f"rt:change:{change.event_id}"] == 604800


def test_missing_change_document_is_a_hard_error():
    client = FakeRedis()
    writer = _writer(client)
    with pytest.raises(KeyError):
        writer.write_changes([_change(1)], {})


def test_serving_writer_issues_no_arithmetic_command():
    client = FakeRedis()
    writer = _writer(client)
    change = _change(1)
    writer.write_changes([change], {change.event_id: {"marketplace": "tiki"}})
    writer.write_source_freshness(marketplace="tiki", last_observation_at=T0, last_change_at=T0)
    assert not set(client.commands) & set(FORBIDDEN_COMMANDS)


def test_source_freshness_stores_timestamps_and_threshold_only():
    client = FakeRedis()
    writer = _writer(client)
    writer.write_source_freshness(marketplace="tiki", last_observation_at=T0)
    stored = client.hashes["rt:source:tiki"]
    assert stored["last_observation_at"] == T0.isoformat()
    assert stored["freshness_threshold_minutes"] == "360"
    assert "last_change_at" not in stored


def test_source_freshness_rejects_naive_timestamp():
    writer = _writer(FakeRedis())
    with pytest.raises(ValueError, match="timezone-aware"):
        writer.write_source_freshness(
            marketplace="tiki", last_observation_at=datetime(2026, 9, 1, 3, 0)
        )


def test_writer_rejects_non_positive_limits():
    with pytest.raises(ValueError):
        MarketplaceRedisWriter(
            FakeRedis(),
            namespace="rt",
            recent_changes_max=0,
            change_doc_ttl_seconds=1,
            freshness_threshold_minutes=1,
        )


def test_no_arithmetic_command_appears_in_source():
    for path in ("serving_layer/marketplace_redis.py", "speed_layer/offer_state.py"):
        source = Path(path).read_text(encoding="utf-8").lower()
        for command in ("hincrby", "zincrby", "incrby"):
            assert f"pipe.{command}" not in source
            assert f"client.{command}" not in source


def test_offer_state_imports_no_engine_library():
    source = Path("speed_layer/offer_state.py").read_text(encoding="utf-8")
    offenders = [
        line.strip()
        for line in source.splitlines()
        if re.match(r"^\s*(import|from)\s", line)
        and any(name in line.lower() for name in ("pyspark", "kafka", "redis", "elasticsearch"))
    ]
    assert offenders == []

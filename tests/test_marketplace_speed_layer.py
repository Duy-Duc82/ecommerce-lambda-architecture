"""Phase 5 SPD-06: micro-batch processing, ordering, audit and replay safety."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from config.marketplace_schema import (
    Availability,
    MarketplaceChangeType,
    create_marketplace_offer,
    create_observation_event,
    create_offer_observation,
)
from speed_layer.change_rules import ChangeThresholds, state_from_observation
from speed_layer.marketplace_speed_layer import (
    STAGE_INDEX,
    STAGE_PUBLISH,
    STAGE_SERVING,
    STAGE_STATE,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    MicroBatchReport,
    postgres_audit_writer,
    process_micro_batch,
    run_stale_sweep,
)
from speed_layer.offer_state import InMemoryOfferStateStore

T0 = datetime(2026, 9, 1, 3, 0, tzinfo=timezone.utc)
CLOCK_T = datetime(2026, 9, 1, 6, 0, tzinfo=timezone.utc)
RULE = "marketplace-change-rules.v1"

THRESHOLDS = ChangeThresholds(
    large_drop_absolute=Decimal("500000"),
    large_drop_percent=Decimal("15"),
    stale_after=timedelta(minutes=1440),
)


def _event(
    *,
    listing="279212151",
    observed_at=T0,
    price="1000000",
    raw_sha=None,
    sold_count=100,
    availability=Availability.IN_STOCK,
):
    raw_sha = raw_sha or (f"{listing}{observed_at.isoformat()}{price}".encode().hex() * 4)[:64].ljust(64, "0")
    offer = create_marketplace_offer(
        marketplace_code="tiki",
        marketplace_id="marketplace-tiki",
        platform_listing_id=listing,
        seller_id=None,
        product_title="MacBook Neo",
        brand="Apple",
        category_path="1/2/1846",
        source_url=f"https://tiki.vn/p-{listing}.html",
        currency="VND",
        first_seen_at=observed_at,
        last_seen_at=observed_at,
    )
    observation = create_offer_observation(
        marketplace_code="tiki",
        platform_listing_id=listing,
        offer_id=offer.offer_id,
        observed_at=observed_at,
        fetched_at=observed_at,
        current_price=Decimal(price),
        raw_uri="s3a://ecommerce-bronze/marketplace/raw/body.bin",
        raw_sha256=raw_sha,
        adapter_version="tiki-listing-v1",
        crawl_run_id="run-1",
        list_price=Decimal("1200000"),
        rating_value=Decimal("4.5"),
        rating_scale=Decimal("5"),
        review_count=10,
        sold_count=sold_count,
        availability=availability,
    )
    return create_observation_event(
        marketplace_code="tiki",
        offer=offer,
        observation=observation,
        platform_listing_id=listing,
        produced_at=observed_at,
    )


class Recorder:
    """Records every sink call in order, so ordering can be asserted."""

    def __init__(self, fail_stage: str | None = None, publish_acked: int = 0):
        self.calls: list[tuple[str, object]] = []
        self.fail_stage = fail_stage
        self.publish_acked = publish_acked

    def publish(self, changes):
        self.calls.append(("publish", tuple(c.event_id for c in changes)))
        if self.fail_stage == STAGE_PUBLISH:
            error = RuntimeError("broker down")
            error.acked = self.publish_acked
            raise error

    def index(self, index, documents):
        self.calls.append(("index", (index, tuple(doc_id for doc_id, _ in documents))))
        if self.fail_stage == STAGE_INDEX:
            raise RuntimeError("elasticsearch down")

    def write_changes(self, changes, documents):
        self.calls.append(("redis_changes", tuple(c.event_id for c in changes)))
        if self.fail_stage == STAGE_SERVING:
            raise RuntimeError("redis down")

    def write_source_freshness(self, **kwargs):
        self.calls.append(("redis_freshness", kwargs.get("marketplace")))
        if self.fail_stage == STAGE_SERVING:
            raise RuntimeError("redis down")

    @property
    def stages(self):
        return [name for name, _ in self.calls]


class FailingStore(InMemoryOfferStateStore):
    def put_many(self, states):
        raise RuntimeError("state store down")


def _records(*events):
    return [{"event": event} for event in events]


def _decoder(record):
    return record["event"]


def _run(
    events,
    *,
    store=None,
    recorder=None,
    audit=None,
    batch_id=7,
    clock_value=CLOCK_T,
):
    store = store if store is not None else InMemoryOfferStateStore()
    recorder = recorder or Recorder()
    return (
        process_micro_batch(
            batch_id=batch_id,
            records=_records(*events),
            state_store=store,
            change_publisher=recorder.publish,
            es_writer=recorder.index,
            redis_writer=recorder,
            thresholds=THRESHOLDS,
            rule_version=RULE,
            clock=lambda: clock_value,
            decoder=_decoder,
            audit_writer=audit,
        ),
        store,
        recorder,
    )


# --- 36: fixed sink order ---------------------------------------------------


def test_sink_calls_happen_in_the_mandatory_order():
    report, store, recorder = _run([_event()])
    assert recorder.stages == [
        "publish",
        "index",
        "index",
        "redis_changes",
        "redis_freshness",
    ]
    assert report.status == STATUS_SUCCEEDED
    assert report.changes_published == 1


def test_state_is_advanced_only_after_publish_and_index():
    events = [_event()]
    store = InMemoryOfferStateStore()
    offer_id = events[0].payload.offer.offer_id

    seen_state_at_publish = {}

    class OrderProbe(Recorder):
        def publish(self, changes):
            seen_state_at_publish["at_publish"] = store.get_many([offer_id])
            return super().publish(changes)

    _run(events, store=store, recorder=OrderProbe())
    assert seen_state_at_publish["at_publish"] == {}
    assert offer_id in store.get_many([offer_id])


# --- 37: intra-batch folding ------------------------------------------------


def test_multiple_observations_of_one_offer_detect_each_transition():
    events = [
        _event(observed_at=T0, price="3000000"),
        _event(observed_at=T0 + timedelta(hours=1), price="2000000"),
        _event(observed_at=T0 + timedelta(hours=2), price="1000000"),
    ]
    report, store, recorder = _run(events)
    assert report.detected == 3
    # NEW_OFFER, then two price changes each with a large drop.
    assert report.changes_by_type == {
        MarketplaceChangeType.NEW_OFFER.value: 1,
        MarketplaceChangeType.PRICE_CHANGED.value: 2,
        MarketplaceChangeType.LARGE_PRICE_DROP.value: 2,
    }
    final = store.get_many([events[0].payload.offer.offer_id])
    assert final[events[0].payload.offer.offer_id].current_price == Decimal("1000000")


def test_out_of_order_records_inside_a_batch_are_sorted_before_folding():
    ordered = [
        _event(observed_at=T0, price="3000000"),
        _event(observed_at=T0 + timedelta(hours=1), price="2000000"),
    ]
    report, _, _ = _run(list(reversed(ordered)))
    assert report.detected == 2
    assert report.out_of_order == 0
    assert report.changes_by_type[MarketplaceChangeType.NEW_OFFER.value] == 1


def test_two_offers_in_one_batch_are_handled_independently():
    report, store, _ = _run([_event(listing="1"), _event(listing="2")])
    assert report.detected == 2
    assert report.changes_by_type == {MarketplaceChangeType.NEW_OFFER.value: 2}


# --- 38: decode failures ----------------------------------------------------


def test_decode_failure_is_counted_and_skipped_without_a_dlq_publish():
    def flaky(record):
        if record.get("broken"):
            raise ValueError("undecodable")
        return record["event"]

    store = InMemoryOfferStateStore()
    recorder = Recorder()
    report = process_micro_batch(
        batch_id=1,
        records=[{"event": _event()}, {"broken": True}],
        state_store=store,
        change_publisher=recorder.publish,
        es_writer=recorder.index,
        redis_writer=recorder,
        thresholds=THRESHOLDS,
        rule_version=RULE,
        clock=lambda: CLOCK_T,
        decoder=flaky,
    )
    assert report.decode_failures == 1
    assert report.observations_in == 2
    assert report.detected == 1
    assert "dlq" not in "".join(recorder.stages).lower()


# --- 39-41: failures leave state untouched ---------------------------------


@pytest.mark.parametrize(
    "fail_stage,expected",
    [
        (STAGE_PUBLISH, STAGE_PUBLISH),
        (STAGE_INDEX, STAGE_INDEX),
        (STAGE_SERVING, STAGE_SERVING),
    ],
)
def test_sink_failure_leaves_stored_state_untouched(fail_stage, expected):
    events = [_event()]
    store = InMemoryOfferStateStore()
    offer_id = events[0].payload.offer.offer_id
    audited: list[MicroBatchReport] = []
    with pytest.raises(RuntimeError):
        _run(events, store=store, recorder=Recorder(fail_stage=fail_stage), audit=audited.append)
    if expected in (STAGE_PUBLISH, STAGE_INDEX):
        assert store.get_many([offer_id]) == {}
    assert audited[0].status == STATUS_FAILED
    assert audited[0].failure_stage == expected


def test_state_store_failure_is_reported_as_the_state_stage():
    audited: list[MicroBatchReport] = []
    with pytest.raises(RuntimeError, match="state store down"):
        _run([_event()], store=FailingStore(), audit=audited.append)
    assert audited[0].failure_stage == STAGE_STATE
    assert audited[0].changes_published == 1


def test_publish_failure_records_the_acked_count():
    audited: list[MicroBatchReport] = []
    with pytest.raises(RuntimeError):
        _run(
            [_event()],
            recorder=Recorder(fail_stage=STAGE_PUBLISH, publish_acked=3),
            audit=audited.append,
        )
    assert audited[0].changes_published == 3


# --- 40: replay after failure is identical ---------------------------------


def test_replay_after_a_publish_failure_emits_identical_change_ids():
    events = [_event()]
    store = InMemoryOfferStateStore()
    first = Recorder(fail_stage=STAGE_PUBLISH)
    with pytest.raises(RuntimeError):
        _run(events, store=store, recorder=first)
    second = Recorder()
    _run(events, store=store, recorder=second)
    failed_ids = [payload for name, payload in first.calls if name == "publish"][0]
    replayed_ids = [payload for name, payload in second.calls if name == "publish"][0]
    assert failed_ids == replayed_ids


def test_replaying_a_processed_observation_is_a_duplicate_with_no_writes():
    events = [_event()]
    store = InMemoryOfferStateStore()
    _run(events, store=store)
    report, _, recorder = _run(events, store=store)
    assert report.duplicates == 1
    assert report.detected == 0
    assert report.changes_published == 0
    assert recorder.stages == []


# --- 42-43: audit -----------------------------------------------------------


def test_successful_batch_writes_a_reconciling_audit_row():
    audited: list[MicroBatchReport] = []
    report, _, _ = _run([_event(listing="1"), _event(listing="2")], audit=audited.append)
    assert audited == [report]
    assert report.status == STATUS_SUCCEEDED
    assert (
        report.detected
        + report.duplicates
        + report.out_of_order
        + report.conflicts
        + report.decode_failures
        == report.observations_in
    )


def test_report_refuses_to_exist_if_counts_do_not_reconcile():
    with pytest.raises(ValueError, match="do not reconcile"):
        MicroBatchReport(
            batch_id=1,
            started_at=CLOCK_T,
            completed_at=CLOCK_T,
            observations_in=5,
            detected=1,
            duplicates=0,
            out_of_order=0,
            conflicts=0,
            decode_failures=0,
            changes_published=1,
        )


def test_failed_report_requires_a_stage_and_success_forbids_one():
    with pytest.raises(ValueError, match="failure stage"):
        MicroBatchReport(
            batch_id=1,
            started_at=CLOCK_T,
            completed_at=CLOCK_T,
            observations_in=0,
            detected=0,
            duplicates=0,
            out_of_order=0,
            conflicts=0,
            decode_failures=0,
            changes_published=0,
            status=STATUS_FAILED,
        )
    with pytest.raises(ValueError, match="cannot carry a failure stage"):
        MicroBatchReport(
            batch_id=1,
            started_at=CLOCK_T,
            completed_at=CLOCK_T,
            observations_in=0,
            detected=0,
            duplicates=0,
            out_of_order=0,
            conflicts=0,
            decode_failures=0,
            changes_published=0,
            status=STATUS_SUCCEEDED,
            failure_stage=STAGE_INDEX,
        )


def test_empty_batch_produces_a_zero_report_and_no_writes():
    report, _, recorder = _run([])
    assert report.observations_in == 0 and report.changes_published == 0
    assert recorder.stages == []


# --- 44-45: stale sweep -----------------------------------------------------


def _sweep(store, recorder, *, now, audit=None, limit=100):
    return run_stale_sweep(
        state_store=store,
        change_publisher=recorder.publish,
        es_writer=recorder.index,
        redis_writer=recorder,
        thresholds=THRESHOLDS,
        rule_version=RULE,
        clock=lambda: now,
        limit=limit,
        audit_writer=audit,
    )


def test_stale_sweep_emits_one_change_and_is_idempotent():
    store = InMemoryOfferStateStore([state_from_observation(_event(observed_at=T0))])
    now = T0 + timedelta(days=3)
    first = _sweep(store, Recorder(), now=now)
    recorder = Recorder()
    second = _sweep(store, recorder, now=now + timedelta(hours=2))
    assert first.changes_published == 1
    assert second.changes_published == 1
    ids_first = first.changes_by_type
    assert ids_first == {MarketplaceChangeType.OFFER_STALE.value: 1}
    published = [payload for name, payload in recorder.calls if name == "publish"][0]
    assert len(published) == 1


def test_stale_sweep_does_not_advance_state():
    state = state_from_observation(_event(observed_at=T0))
    store = InMemoryOfferStateStore([state])
    _sweep(store, Recorder(), now=T0 + timedelta(days=3))
    assert store.get_many([state.offer_id])[state.offer_id] == state


def test_fresh_observation_after_a_stale_change_allows_a_later_stale_change():
    old = _event(observed_at=T0)
    store = InMemoryOfferStateStore([state_from_observation(old)])
    first = _sweep(store, Recorder(), now=T0 + timedelta(days=3))
    fresh = _event(observed_at=T0 + timedelta(days=3), price="900000")
    _run([fresh], store=store)
    recorder = Recorder()
    second = _sweep(store, recorder, now=T0 + timedelta(days=9))
    assert first.changes_published == 1 and second.changes_published == 1
    published = [payload for name, payload in recorder.calls if name == "publish"][0]
    assert published[0] != "", "a new stale change exists for the newer observation"


def test_stale_sweep_of_a_fresh_store_publishes_nothing():
    store = InMemoryOfferStateStore([state_from_observation(_event(observed_at=T0))])
    recorder = Recorder()
    report = _sweep(store, recorder, now=T0 + timedelta(minutes=5))
    assert report.changes_published == 0
    assert report.changes_by_type == {}


def test_stale_sweep_failure_is_audited_by_stage():
    store = InMemoryOfferStateStore([state_from_observation(_event(observed_at=T0))])
    audited: list[MicroBatchReport] = []
    with pytest.raises(RuntimeError):
        _sweep(store, Recorder(fail_stage=STAGE_PUBLISH), now=T0 + timedelta(days=3), audit=audited.append)
    assert audited[0].failure_stage == STAGE_PUBLISH


# --- 46: import and purity constraints -------------------------------------


def test_module_imports_without_pyspark_installed():
    import importlib

    with pytest.raises(ImportError):
        importlib.import_module("pyspark")
    module = importlib.import_module("speed_layer.marketplace_speed_layer")
    assert hasattr(module, "process_micro_batch")


def test_pyspark_is_never_imported_at_module_scope():
    source = Path("speed_layer/marketplace_speed_layer.py").read_text(encoding="utf-8")
    module_level = [
        line
        for line in source.splitlines()
        if re.match(r"^(import|from)\s", line) and "pyspark" in line
    ]
    assert module_level == []


def test_foreach_batch_exception_is_not_swallowed():
    source = Path("speed_layer/marketplace_speed_layer.py").read_text(encoding="utf-8")
    handler = source.split("def handle_batch")[1].split("query = (")[0]
    # an `except` statement, not the word inside the explanatory comment
    assert [line for line in handler.splitlines() if re.match(r"^\s*except", line)] == []


def test_postgres_audit_writer_upserts_on_rule_version_and_batch():
    statements: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params=None):
            statements.append(" ".join(sql.split()))

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def cursor(self):
            return Cursor()

        def close(self):
            statements.append("CLOSE")

    write = postgres_audit_writer(lambda: Connection())
    write(
        MicroBatchReport(
            batch_id=3,
            started_at=CLOCK_T,
            completed_at=CLOCK_T,
            observations_in=0,
            detected=0,
            duplicates=0,
            out_of_order=0,
            conflicts=0,
            decode_failures=0,
            changes_published=0,
        )
    )
    assert "INSERT INTO audit.speed_micro_batch" in statements[0]
    assert "ON CONFLICT (rule_version, batch_id) DO UPDATE" in statements[0]
    assert statements[-1] == "CLOSE"

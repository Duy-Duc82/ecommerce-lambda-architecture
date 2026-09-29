"""Scheduling and retry tests, items 1-11 of the Phase 3 plan section 12.

Every time here is a fixed UTC instant and no test sleeps.
"""
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from config.marketplace_schema import ResourceType
from config.settings import (
    CRAWL_ACTIVE_CADENCE_MINUTES,
    CRAWL_COLD_CADENCE_MINUTES,
    CRAWL_NORMAL_CADENCE_MINUTES,
)
from crawler.contracts import (
    CanonicalRecordError,
    FetchTransportError,
    HttpResponseError,
    ListingPageParseError,
    RawPersistenceError,
    RobotsDeniedError,
)
from crawler.scheduling import (
    CrawlTask,
    CrawlTaskStatus,
    CrawlTier,
    FailureKind,
    RetryDecision,
    RetryPolicy,
    cadence_minutes,
    classify_failure,
    decide_retry,
    make_crawl_task_id,
    next_scheduled_for,
    parse_retry_after,
)

NOW = datetime(2026, 9, 29, 8, 0, tzinfo=timezone.utc)
BUCKET = datetime(2026, 9, 29, 8, 0, tzinfo=timezone.utc)
POLICY = RetryPolicy(5, 30, 1800, Decimal("0.20"))
NO_JITTER = Decimal("0.5")


def _task_id(**overrides):
    values = {
        "marketplace_code": "tiki",
        "resource_type": ResourceType.LISTING_PAGE,
        "target": "1846:1",
        "scheduled_for": BUCKET,
    }
    values.update(overrides)
    return make_crawl_task_id(**values)


def _decide(error, *, http_status=None, retry_after=None, attempts=1, policy=POLICY,
            jitter_unit=NO_JITTER):
    return decide_retry(
        error=error,
        http_status=http_status,
        retry_after=retry_after,
        attempts=attempts,
        now=NOW,
        policy=policy,
        jitter_unit=jitter_unit,
    )


# 1
def test_task_ids_are_deterministic_and_prefixed_hex():
    task_id = _task_id()

    assert task_id == _task_id()
    assert re.fullmatch(r"task_[0-9a-f]{64}", task_id)


# 2
@pytest.mark.parametrize(
    "overrides",
    [
        {"target": "1846:2"},
        {"scheduled_for": BUCKET + timedelta(minutes=60)},
        {"resource_type": ResourceType.PRODUCT_DETAIL},
        {"marketplace_code": "yame"},
    ],
)
def test_changing_target_bucket_or_resource_changes_the_id(overrides):
    assert _task_id(**overrides) != _task_id()


# 3
def test_timezone_equivalent_buckets_create_the_same_id():
    tehran = timezone(timedelta(hours=3, minutes=30))

    assert _task_id(scheduled_for=BUCKET.astimezone(tehran)) == _task_id()


def test_marketplace_code_case_and_padding_do_not_change_the_id():
    assert _task_id(marketplace_code=" TIKI ") == _task_id()


# 4
def test_naive_timestamps_fail():
    with pytest.raises(ValueError, match="timezone-aware"):
        _task_id(scheduled_for=datetime(2026, 9, 29, 8, 0))


@pytest.mark.parametrize("overrides", [{"target": "  "}, {"marketplace_code": ""}])
def test_blank_identity_parts_fail(overrides):
    with pytest.raises(ValueError):
        _task_id(**overrides)


def test_resource_type_must_be_an_enum():
    with pytest.raises(ValueError, match="resource_type"):
        _task_id(resource_type="LISTING_PAGE")


# 5
def test_tier_cadences_use_the_configured_values():
    assert cadence_minutes(CrawlTier.ACTIVE) == CRAWL_ACTIVE_CADENCE_MINUTES
    assert cadence_minutes(CrawlTier.NORMAL) == CRAWL_NORMAL_CADENCE_MINUTES
    assert cadence_minutes(CrawlTier.COLD) == CRAWL_COLD_CADENCE_MINUTES
    assert cadence_minutes(CrawlTier.ACTIVE) < cadence_minutes(CrawlTier.COLD)


def test_the_next_occurrence_is_one_cadence_later():
    assert next_scheduled_for(CrawlTier.ACTIVE, NOW) == NOW + timedelta(
        minutes=CRAWL_ACTIVE_CADENCE_MINUTES
    )


# 6
@pytest.mark.parametrize(
    "error, http_status, expected",
    [
        (FetchTransportError("reset"), None, FailureKind.TRANSIENT_NETWORK),
        (TimeoutError("timed out"), None, FailureKind.TRANSIENT_NETWORK),
        (ConnectionResetError("reset"), None, FailureKind.TRANSIENT_NETWORK),
        (HttpResponseError(408), None, FailureKind.TRANSIENT_NETWORK),
        (HttpResponseError(425), None, FailureKind.TRANSIENT_NETWORK),
        (HttpResponseError(429), None, FailureKind.RATE_LIMITED),
        (HttpResponseError(500), None, FailureKind.SERVER_ERROR),
        (HttpResponseError(503), None, FailureKind.SERVER_ERROR),
        (RawPersistenceError("s3 down", write_stage="BODY"), None, FailureKind.STORAGE_ERROR),
    ],
)
def test_transient_rate_limited_and_server_failures_are_retryable(
    error, http_status, expected
):
    decision = _decide(error, http_status=http_status)

    assert classify_failure(error, http_status) is expected
    assert decision.retryable is True
    assert decision.failure_kind is expected
    assert decision.next_attempt_at == NOW + timedelta(seconds=decision.delay_seconds)


# 7
@pytest.mark.parametrize(
    "error, http_status, expected",
    [
        (HttpResponseError(400), None, FailureKind.CLIENT_ERROR),
        (HttpResponseError(403), None, FailureKind.CLIENT_ERROR),
        (HttpResponseError(404), None, FailureKind.CLIENT_ERROR),
        (RobotsDeniedError("disallowed"), None, FailureKind.ROBOTS_DENIED),
        (ListingPageParseError("schema drift"), None, FailureKind.PARSE_ERROR),
        (CanonicalRecordError("bad price"), None, FailureKind.VALIDATION_ERROR),
        (KeyError("programming error"), None, FailureKind.UNKNOWN),
    ],
)
def test_ordinary_client_robots_parse_and_validation_failures_are_terminal(
    error, http_status, expected
):
    decision = _decide(error, http_status=http_status)

    assert classify_failure(error, http_status) is expected
    assert decision.retryable is False
    assert decision.delay_seconds is None
    assert decision.next_attempt_at is None


def test_robots_denial_stays_terminal_even_with_a_retryable_status():
    assert classify_failure(RobotsDeniedError("no"), 503) is FailureKind.ROBOTS_DENIED


# 8
def test_exponential_delay_is_capped():
    policy = RetryPolicy(10, 30, 120, Decimal("0"))
    delays = [
        _decide(HttpResponseError(500), attempts=n, policy=policy).delay_seconds
        for n in range(1, 7)
    ]

    assert delays == [30, 60, 120, 120, 120, 120]


def test_the_first_failure_waits_the_base_delay():
    policy = RetryPolicy(5, 30, 1800, Decimal("0"))

    assert _decide(HttpResponseError(500), attempts=1, policy=policy).delay_seconds == 30


# 9
def test_jitter_lower_and_upper_bounds_are_deterministic():
    policy = RetryPolicy(5, 100, 1800, Decimal("0.20"))

    lowest = _decide(HttpResponseError(500), policy=policy, jitter_unit=Decimal("0"))
    middle = _decide(HttpResponseError(500), policy=policy, jitter_unit=Decimal("0.5"))
    highest = _decide(HttpResponseError(500), policy=policy, jitter_unit=Decimal("1"))

    assert lowest.delay_seconds == 80
    assert middle.delay_seconds == 100
    assert highest.delay_seconds == 120
    assert lowest.delay_seconds == _decide(
        HttpResponseError(500), policy=policy, jitter_unit=Decimal("0")
    ).delay_seconds


def test_jitter_never_returns_less_than_one_second():
    policy = RetryPolicy(5, 1, 1800, Decimal("1"))

    assert _decide(
        HttpResponseError(500), policy=policy, jitter_unit=Decimal("0")
    ).delay_seconds == 1


def test_a_jitter_unit_outside_the_unit_interval_is_rejected():
    with pytest.raises(ValueError, match="jitter_unit"):
        _decide(HttpResponseError(500), jitter_unit=Decimal("1.5"))


# 10
def test_retry_after_seconds_is_honoured_and_capped():
    policy = RetryPolicy(5, 30, 120, Decimal("0"))

    honoured = _decide(HttpResponseError(429, "45"), retry_after="45", policy=policy)
    capped = _decide(HttpResponseError(429, "600"), retry_after="600", policy=policy)

    assert honoured.delay_seconds == 45
    assert capped.delay_seconds == 120


def test_retry_after_http_date_is_honoured():
    later = NOW + timedelta(seconds=90)
    header = later.strftime("%a, %d %b %Y %H:%M:%S GMT")

    assert parse_retry_after(header, NOW) == 90
    assert _decide(HttpResponseError(503), retry_after=header).delay_seconds == 90


def test_a_retry_after_date_in_the_past_waits_no_negative_time():
    header = (NOW - timedelta(hours=1)).strftime("%a, %d %b %Y %H:%M:%S GMT")

    assert parse_retry_after(header, NOW) == 0
    assert _decide(HttpResponseError(503), retry_after=header).delay_seconds == 1


@pytest.mark.parametrize("value", [None, "", "   ", "soon", "-5"])
def test_an_unusable_retry_after_falls_back_to_the_exponential_delay(value):
    policy = RetryPolicy(5, 30, 1800, Decimal("0"))

    assert parse_retry_after(value, NOW) is None
    assert _decide(
        HttpResponseError(503), retry_after=value, policy=policy
    ).delay_seconds == 30


# 11
def test_exhausted_attempts_never_retry():
    decision = _decide(HttpResponseError(503), attempts=POLICY.max_attempts)

    assert decision.retryable is False
    assert decision.failure_kind is FailureKind.SERVER_ERROR
    assert decision.delay_seconds is None


def test_the_last_allowed_attempt_still_retries():
    assert _decide(HttpResponseError(503), attempts=POLICY.max_attempts - 1).retryable


@pytest.mark.parametrize("attempts", [0, -1, True])
def test_a_non_positive_attempt_count_is_rejected(attempts):
    with pytest.raises(ValueError, match="attempts"):
        _decide(HttpResponseError(503), attempts=attempts)


# Contract guards on the dataclasses themselves.
def _crawl_task(**overrides):
    values = {
        "task_id": _task_id(),
        "marketplace_code": "tiki",
        "marketplace_id": "marketplace-tiki",
        "target": "1846:1",
        "resource_type": ResourceType.LISTING_PAGE,
        "tier": CrawlTier.NORMAL,
        "priority": 10,
        "scheduled_for": BUCKET,
        "status": CrawlTaskStatus.READY,
        "attempts": 0,
        "max_attempts": 5,
    }
    values.update(overrides)
    return CrawlTask(**values)


def test_a_leased_task_requires_both_lease_fields():
    with pytest.raises(ValueError, match="LEASED requires"):
        _crawl_task(status=CrawlTaskStatus.LEASED)

    leased = _crawl_task(
        status=CrawlTaskStatus.LEASED,
        lease_owner="worker-1",
        lease_expires_at=NOW + timedelta(seconds=300),
    )
    assert leased.lease_owner == "worker-1"


def test_a_task_that_is_not_leased_must_not_hold_a_lease():
    with pytest.raises(ValueError, match="must not hold a lease"):
        _crawl_task(status=CrawlTaskStatus.READY, lease_owner="worker-1")


def test_task_timestamps_normalize_to_utc():
    tehran = timezone(timedelta(hours=3, minutes=30))

    task = _crawl_task(scheduled_for=BUCKET.astimezone(tehran))

    assert task.scheduled_for == BUCKET
    assert task.scheduled_for.tzinfo == timezone.utc


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"priority": -1}, "priority"),
        ({"attempts": True}, "attempts"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"attempts": 6, "max_attempts": 5}, "cannot exceed"),
        ({"tier": "NORMAL"}, "tier"),
        ({"status": "READY"}, "status"),
        ({"last_http_status": 99}, "last_http_status"),
    ],
)
def test_task_rejects_invalid_values(overrides, message):
    with pytest.raises(ValueError, match=message):
        _crawl_task(**overrides)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"max_attempts": 0}, "max_attempts"),
        ({"base_seconds": 0}, "base_seconds"),
        ({"max_seconds": 10, "base_seconds": 30}, "at least base_seconds"),
        ({"jitter_ratio": Decimal("1.5")}, "jitter_ratio"),
        ({"jitter_ratio": 0.2}, "Decimal"),
    ],
)
def test_retry_policy_rejects_invalid_values(overrides, message):
    values = {
        "max_attempts": 5,
        "base_seconds": 30,
        "max_seconds": 1800,
        "jitter_ratio": Decimal("0.20"),
    }
    values.update(overrides)

    with pytest.raises(ValueError, match=message):
        RetryPolicy(**values)


def test_a_terminal_decision_cannot_carry_a_delay():
    with pytest.raises(ValueError, match="terminal decision"):
        RetryDecision(False, FailureKind.CLIENT_ERROR, 30, NOW)


def test_a_retryable_decision_must_carry_a_delay():
    with pytest.raises(ValueError, match="needs a delay"):
        RetryDecision(True, FailureKind.SERVER_ERROR, None, None)

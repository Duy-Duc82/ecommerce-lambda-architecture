"""Pure scheduling, retry and failure-classification rules for the crawler.

Nothing here touches PostgreSQL, the network or a clock of its own: the
frontier persists what these functions decide, and the worker supplies the
time. Keeping the decisions pure is what lets a retry schedule be replayed
and asserted exactly.
"""
from __future__ import annotations

import socket
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from enum import Enum

from common.identity import deterministic_id
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


class CrawlTier(str, Enum):
    ACTIVE = "ACTIVE"
    NORMAL = "NORMAL"
    COLD = "COLD"


class CrawlTaskStatus(str, Enum):
    READY = "READY"
    LEASED = "LEASED"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DISABLED = "DISABLED"


class FailureKind(str, Enum):
    RATE_LIMITED = "RATE_LIMITED"
    TRANSIENT_NETWORK = "TRANSIENT_NETWORK"
    SERVER_ERROR = "SERVER_ERROR"
    CLIENT_ERROR = "CLIENT_ERROR"
    ROBOTS_DENIED = "ROBOTS_DENIED"
    STORAGE_ERROR = "STORAGE_ERROR"
    PARSE_ERROR = "PARSE_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    UNKNOWN = "UNKNOWN"


_RETRYABLE_KINDS = frozenset(
    {
        FailureKind.RATE_LIMITED,
        FailureKind.TRANSIENT_NETWORK,
        FailureKind.SERVER_ERROR,
        FailureKind.STORAGE_ERROR,
    }
)
# 408 Request Timeout, 425 Too Early and 429 Too Many Requests are the only
# 4xx codes a client may retry without changing the request.
_RETRYABLE_CLIENT_STATUS = frozenset({408, 425, 429})


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _non_bool_int(value: object, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


@dataclass(frozen=True)
class CrawlTask:
    task_id: str
    marketplace_code: str
    marketplace_id: str
    target: str
    resource_type: ResourceType
    tier: CrawlTier
    priority: int
    scheduled_for: datetime
    status: CrawlTaskStatus
    attempts: int
    max_attempts: int
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    last_http_status: int | None = None
    last_error_kind: FailureKind | None = None
    last_error: str | None = None
    last_success_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("task_id", "marketplace_code", "marketplace_id", "target"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        for name, enum_type in (
            ("resource_type", ResourceType),
            ("tier", CrawlTier),
            ("status", CrawlTaskStatus),
        ):
            if not isinstance(getattr(self, name), enum_type):
                raise ValueError(f"{name} must be a {enum_type.__name__}")
        if self.last_error_kind is not None and not isinstance(
            self.last_error_kind, FailureKind
        ):
            raise ValueError("last_error_kind must be a FailureKind")
        object.__setattr__(self, "priority", _non_bool_int(self.priority, "priority"))
        object.__setattr__(self, "attempts", _non_bool_int(self.attempts, "attempts"))
        object.__setattr__(
            self,
            "max_attempts",
            _non_bool_int(self.max_attempts, "max_attempts", minimum=1),
        )
        if self.attempts > self.max_attempts:
            raise ValueError("attempts cannot exceed max_attempts")
        object.__setattr__(
            self, "scheduled_for", _aware_utc(self.scheduled_for, "scheduled_for")
        )
        for name in ("lease_expires_at", "last_success_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _aware_utc(value, name))
        if self.status is CrawlTaskStatus.LEASED:
            if not self.lease_owner or self.lease_expires_at is None:
                raise ValueError("LEASED requires lease_owner and lease_expires_at")
        elif self.lease_owner is not None or self.lease_expires_at is not None:
            raise ValueError(f"{self.status.value} must not hold a lease")
        if self.last_http_status is not None:
            status = _non_bool_int(self.last_http_status, "last_http_status", minimum=100)
            if status > 599:
                raise ValueError("last_http_status must be a valid HTTP status")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    base_seconds: int
    max_seconds: int
    jitter_ratio: Decimal

    def __post_init__(self) -> None:
        for name, minimum in (
            ("max_attempts", 1),
            ("base_seconds", 1),
            ("max_seconds", 1),
        ):
            object.__setattr__(
                self, name, _non_bool_int(getattr(self, name), name, minimum=minimum)
            )
        if self.max_seconds < self.base_seconds:
            raise ValueError("max_seconds must be at least base_seconds")
        if not isinstance(self.jitter_ratio, Decimal):
            raise ValueError("jitter_ratio must be a Decimal")
        if not Decimal("0") <= self.jitter_ratio <= Decimal("1"):
            raise ValueError("jitter_ratio must be between 0 and 1")


@dataclass(frozen=True)
class RetryDecision:
    retryable: bool
    failure_kind: FailureKind
    delay_seconds: int | None
    next_attempt_at: datetime | None

    def __post_init__(self) -> None:
        if not isinstance(self.failure_kind, FailureKind):
            raise ValueError("failure_kind must be a FailureKind")
        if self.retryable:
            if self.delay_seconds is None or self.next_attempt_at is None:
                raise ValueError("a retryable decision needs a delay and a time")
            _non_bool_int(self.delay_seconds, "delay_seconds", minimum=1)
            object.__setattr__(
                self, "next_attempt_at", _aware_utc(self.next_attempt_at, "next_attempt_at")
            )
        elif self.delay_seconds is not None or self.next_attempt_at is not None:
            raise ValueError("a terminal decision carries no delay")


def make_crawl_task_id(
    marketplace_code: str,
    resource_type: ResourceType,
    target: str,
    scheduled_for: datetime,
) -> str:
    """Identity of one scheduled occurrence of a target.

    The caller passes an already-bucketed ``scheduled_for``; this function
    never truncates time, so the cadence policy stays in one place.
    """
    if not isinstance(resource_type, ResourceType):
        raise ValueError("resource_type must be a ResourceType")
    return deterministic_id(
        "task",
        _required_text(marketplace_code, "marketplace_code").lower(),
        resource_type,
        _required_text(target, "target"),
        _aware_utc(scheduled_for, "scheduled_for"),
    )


def cadence_minutes(tier: CrawlTier) -> int:
    if not isinstance(tier, CrawlTier):
        raise ValueError("tier must be a CrawlTier")
    return {
        CrawlTier.ACTIVE: CRAWL_ACTIVE_CADENCE_MINUTES,
        CrawlTier.NORMAL: CRAWL_NORMAL_CADENCE_MINUTES,
        CrawlTier.COLD: CRAWL_COLD_CADENCE_MINUTES,
    }[tier]


def next_scheduled_for(tier: CrawlTier, after: datetime) -> datetime:
    """The next occurrence of a target one cadence after the given time."""
    return _aware_utc(after, "after") + timedelta(minutes=cadence_minutes(tier))


def classify_failure(error: Exception, http_status: int | None = None) -> FailureKind:
    """Map a Phase 2 boundary failure onto the persisted error taxonomy."""
    if isinstance(error, RobotsDeniedError):
        return FailureKind.ROBOTS_DENIED
    if isinstance(error, RawPersistenceError):
        return FailureKind.STORAGE_ERROR
    if isinstance(error, ListingPageParseError):
        return FailureKind.PARSE_ERROR
    if isinstance(error, CanonicalRecordError):
        return FailureKind.VALIDATION_ERROR
    if isinstance(error, HttpResponseError):
        http_status = http_status if http_status is not None else error.status_code
    if isinstance(error, (FetchTransportError, TimeoutError, socket.timeout, ConnectionError)):
        # A transport error that already carries a status is classified by it.
        if http_status is None:
            return FailureKind.TRANSIENT_NETWORK
    if http_status is not None:
        if http_status == 429:
            return FailureKind.RATE_LIMITED
        if http_status in _RETRYABLE_CLIENT_STATUS:
            return FailureKind.TRANSIENT_NETWORK
        if 500 <= http_status <= 599:
            return FailureKind.SERVER_ERROR
        if 400 <= http_status <= 499:
            return FailureKind.CLIENT_ERROR
    if isinstance(error, (FetchTransportError, TimeoutError, ConnectionError)):
        return FailureKind.TRANSIENT_NETWORK
    return FailureKind.UNKNOWN


def parse_retry_after(value: str | None, now: datetime) -> int | None:
    """Seconds to wait from a Retry-After header, or None when unusable.

    Accepts both forms RFC 9110 allows: delay-seconds and an HTTP-date. A date
    already in the past yields zero, never a negative wait.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        return None
    now = _aware_utc(now, "now")
    text = value.strip()
    if text.lstrip("+").isdigit():
        seconds = int(text)
        return seconds if seconds >= 0 else None
    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if moment is None:
        return None
    if moment.tzinfo is None or moment.utcoffset() is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return max(0, int((moment - now).total_seconds()))


def _jittered(delay: int, jitter_ratio: Decimal, jitter_unit: Decimal) -> int:
    """Apply a deterministic jitter in [-ratio, +ratio] using an injected unit."""
    if not isinstance(jitter_unit, Decimal):
        raise ValueError("jitter_unit must be a Decimal")
    if not Decimal("0") <= jitter_unit <= Decimal("1"):
        raise ValueError("jitter_unit must be between 0 and 1")
    offset = (jitter_unit * 2 - 1) * jitter_ratio
    jittered = Decimal(delay) * (Decimal(1) + offset)
    whole = int(jittered.to_integral_value(rounding="ROUND_CEILING"))
    return max(1, whole)


def decide_retry(
    *,
    error: Exception,
    http_status: int | None,
    retry_after: str | None,
    attempts: int,
    now: datetime,
    policy: RetryPolicy,
    jitter_unit: Decimal,
) -> RetryDecision:
    """Decide whether a failed attempt is rescheduled, and for when.

    ``attempts`` is the count already consumed and starts at one, so the first
    failure waits ``base_seconds`` before jitter.
    """
    if not isinstance(error, Exception):
        raise TypeError("error must be an exception")
    if not isinstance(policy, RetryPolicy):
        raise TypeError("policy must be a RetryPolicy")
    attempts = _non_bool_int(attempts, "attempts", minimum=1)
    now = _aware_utc(now, "now")
    kind = classify_failure(error, http_status)

    if kind not in _RETRYABLE_KINDS or attempts >= policy.max_attempts:
        return RetryDecision(False, kind, None, None)

    header = parse_retry_after(retry_after, now)
    if header is not None:
        delay = min(max(1, header), policy.max_seconds)
    else:
        exponential = policy.base_seconds * 2 ** (attempts - 1)
        delay = _jittered(
            min(policy.max_seconds, exponential), policy.jitter_ratio, jitter_unit
        )
        delay = min(delay, policy.max_seconds)
    return RetryDecision(True, kind, delay, now + timedelta(seconds=delay))

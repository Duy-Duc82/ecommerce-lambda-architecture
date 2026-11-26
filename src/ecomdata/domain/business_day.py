"""The business day: the calendar date in Vietnam time.

Timestamps stay in UTC everywhere (Kafka, Bronze paths, Silver columns, the
Spark session). Only the *day* a row is counted under is Vietnam's, so a
sighting at 23:30 on 14/11 in Hanoi (16:30 UTC) and one at 00:30 on 15/11
(17:30 UTC on 14/11) land on different days, as a reader in Vietnam expects.
Vietnam has no daylight saving, so the offset is a fixed +7 hours.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

BUSINESS_TZ_NAME = "Asia/Ho_Chi_Minh"
try:
    BUSINESS_TZ: timezone | ZoneInfo = ZoneInfo(BUSINESS_TZ_NAME)
except Exception:  # pragma: no cover - Windows without tzdata
    BUSINESS_TZ = timezone(timedelta(hours=7), "ICT")


def business_date(moment: datetime) -> date:
    """The Vietnam calendar date of an aware ``moment``."""
    if moment.tzinfo is None:
        raise ValueError("business_date needs a timezone-aware datetime")
    return moment.astimezone(BUSINESS_TZ).date()

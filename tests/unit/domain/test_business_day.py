from datetime import UTC, date, datetime

import pytest

from ecomdata.domain.business_day import business_date


def test_the_business_day_turns_at_vietnam_midnight():
    assert business_date(datetime(2026, 11, 14, 16, 59, tzinfo=UTC)) == date(2026, 11, 14)
    assert business_date(datetime(2026, 11, 14, 17, 0, tzinfo=UTC)) == date(2026, 11, 15)


def test_a_naive_datetime_is_refused():
    with pytest.raises(ValueError, match="timezone-aware"):
        business_date(datetime(2026, 11, 14, 12, 0))

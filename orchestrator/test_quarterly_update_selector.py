from datetime import datetime

from quarterly_update_selector import _requires_quarterly_refresh


def test_requires_quarterly_refresh_when_missing_timestamp():
    now = datetime(2026, 4, 1, 0, 0, 0)
    assert _requires_quarterly_refresh(None, now) is True


def test_requires_quarterly_refresh_when_new_quarter():
    now = datetime(2026, 4, 1, 0, 0, 0)
    last_updated = datetime(2026, 3, 31, 23, 59, 59)
    assert _requires_quarterly_refresh(last_updated, now) is True


def test_requires_quarterly_refresh_when_same_quarter():
    now = datetime(2026, 5, 15, 12, 0, 0)
    last_updated = datetime(2026, 4, 10, 9, 0, 0)
    assert _requires_quarterly_refresh(last_updated, now) is False

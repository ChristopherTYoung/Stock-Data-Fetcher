from datetime import datetime

try:
    from .quarterly_update_selector import _requires_quarterly_refresh
except ImportError:
    from quarterly_update_selector import _requires_quarterly_refresh


def test_requires_quarterly_refresh_when_missing_timestamp():
    now = datetime(2026, 4, 1, 0, 0, 0)
    assert _requires_quarterly_refresh(None, False, now) is True


def test_requires_quarterly_refresh_when_new_quarter():
    now = datetime(2026, 4, 1, 0, 0, 0)
    last_updated = datetime(2026, 3, 31, 23, 59, 59)
    assert _requires_quarterly_refresh(last_updated, False, now) is True


def test_requires_quarterly_refresh_when_same_quarter():
    now = datetime(2026, 5, 15, 12, 0, 0)
    last_updated = datetime(2026, 4, 10, 9, 0, 0)
    assert _requires_quarterly_refresh(last_updated, False, now) is False


def test_requires_quarterly_refresh_when_same_quarter_but_missing_data():
    now = datetime(2026, 5, 15, 12, 0, 0)
    last_updated = datetime(2026, 5, 1, 9, 0, 0)
    assert _requires_quarterly_refresh(last_updated, True, now) is True

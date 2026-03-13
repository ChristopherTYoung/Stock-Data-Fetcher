from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

import polygon_stock_service
from database import Stock, get_db


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class _FakeVx:
    def __init__(self, reports):
        self._reports = reports

    def list_stock_financials(self, **kwargs):
        return iter(self._reports)


class _FakeClient:
    def __init__(self, reports, bars, details):
        self.vx = _FakeVx(reports)
        self._bars = bars
        self._details = details

    def get_ticker_details(self, ticker):
        return self._details

    def list_aggs(self, **kwargs):
        return iter(self._bars)


@pytest.fixture
def fake_details():
    return SimpleNamespace(
        name="Test Corp",
        list_date=None,
        description=None,
        market_cap=None,
        primary_exchange=None,
        type=None,
        currency_name=None,
        cik=None,
        composite_figi=None,
        share_class_figi=None,
        weighted_shares_outstanding=None,
        homepage_url=None,
        total_employees=None,
        locale=None,
        sic_code=None,
        sic_description=None,
    )


def _make_report(year: int, eps: float):
    income_statement = SimpleNamespace(
        basic_earnings_per_share=SimpleNamespace(value=eps)
    )
    balance_sheet = SimpleNamespace(
        long_term_debt=None,
        noncurrent_liabilities=None,
        equity=None,
    )
    financials = SimpleNamespace(
        income_statement=income_statement,
        balance_sheet=balance_sheet,
    )
    return SimpleNamespace(
        fiscal_year=year,
        end_date=f"{year}-12-31",
        financials=financials,
    )


def test_update_stocks_persists_calculated_fields(monkeypatch, fake_details):
    now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    yesterday = now - timedelta(days=1)

    bars = [
        SimpleNamespace(timestamp=_to_ms(yesterday), open=99, high=105, low=95, close=100, volume=1000),
        SimpleNamespace(timestamp=_to_ms(now), open=108, high=120, low=90, close=110, volume=1200),
    ]

    current_year = datetime.now().year
    reports = [
        _make_report(current_year, 2.0),
        _make_report(current_year - 1, 1.0),
    ]

    fake_client = _FakeClient(reports=reports, bars=bars, details=fake_details)

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(polygon_stock_service, "RESTClient", lambda api_key: fake_client)

    updated = polygon_stock_service.update_stocks_in_db_from_polygon([{"symbol": "TEST"}])
    assert updated == 1

    with get_db() as db:
        row = db.query(Stock).filter(Stock.symbol == "TEST").first()
        assert row is not None
        assert row.price == 11000
        assert row.high52 == 12000
        assert row.low52 == 9000
        assert row.percent_change == 1000
        assert row.annual_eps_growth_rate == 100
        assert row.price_per_earnings == 5500
        assert row.pe_per_growth == 55


def test_update_stocks_handles_missing_growth_denominator(monkeypatch, fake_details):
    now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    yesterday = now - timedelta(days=1)
    bars = [
        SimpleNamespace(timestamp=_to_ms(yesterday), open=99, high=101, low=95, close=100, volume=1000),
        SimpleNamespace(timestamp=_to_ms(now), open=101, high=103, low=99, close=102, volume=1100),
    ]

    current_year = datetime.now().year

    reports = [
        _make_report(current_year, 1.0),
        _make_report(current_year - 1, 0.0),
    ]

    fake_client = _FakeClient(reports=reports, bars=bars, details=fake_details)

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(polygon_stock_service, "RESTClient", lambda api_key: fake_client)

    updated = polygon_stock_service.update_stocks_in_db_from_polygon([{"symbol": "NOGROW"}])
    assert updated == 1

    with get_db() as db:
        row = db.query(Stock).filter(Stock.symbol == "NOGROW").first()
        assert row is not None
        assert row.annual_eps_growth_rate is None
        assert row.pe_per_growth is None

from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
import pandas as pd

from stock_data_calculator import polygon_stock_service
from stock_data_calculator.database import Stock, get_db


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


class _FakeYfinanceTicker:
    def __init__(
        self,
        shares_outstanding=None,
        trailing_eps=None,
        total_revenue=None,
        net_income_4q_ago=None,
        trailing_pe=None,
        peg_ratio=None,
        debt_to_equity=None,
    ):
        self.shares_outstanding = shares_outstanding
        self.trailing_eps = trailing_eps
        self.total_revenue = total_revenue
        self.net_income_4q_ago = net_income_4q_ago
        self.info = {
            'sharesOutstanding': shares_outstanding,
            'trailingEps': trailing_eps,
            'totalRevenue': total_revenue,
            'trailingPE': trailing_pe,
            'priceToSalesTrailing12Months': None,
            'pegRatio': peg_ratio,
            'debtToEquity': debt_to_equity,
        }
        
        if net_income_4q_ago is not None:
            self.quarterly_financials = pd.DataFrame(
                {
                    'Q0': [0],
                    'Q1': [0],
                    'Q2': [0],
                    'Q3': [0],
                    'Q4': [net_income_4q_ago],
                },
                index=['Net Income']
            )
        else:
            self.quarterly_financials = pd.DataFrame()


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
        basic_earnings_per_share=SimpleNamespace(value=eps),
        total_revenue=SimpleNamespace(value=2_000_000_000),
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
    fake_details.weighted_shares_outstanding = 100_000_000
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
    
    fake_yf_ticker = _FakeYfinanceTicker(
        shares_outstanding=100_000_000,
        trailing_eps=2.0,
        total_revenue=2_000_000_000,
        net_income_4q_ago=100_000_000,
        trailing_pe=55.0,
        peg_ratio=0.55,
    )
    monkeypatch.setattr(polygon_stock_service.yf, "Ticker", lambda ticker: fake_yf_ticker)

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
        assert row.revenue_per_share == Decimal("20.00")
        assert row.price_per_sales == Decimal("5.50")


def test_update_stocks_handles_missing_growth_denominator(monkeypatch, fake_details):
    fake_details.weighted_shares_outstanding = 100_000_000
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
    
    fake_yf_ticker = _FakeYfinanceTicker(
        shares_outstanding=100_000_000,
        trailing_eps=1.0,
        total_revenue=2_000_000_000,
        net_income_4q_ago=0
    )
    monkeypatch.setattr(polygon_stock_service.yf, "Ticker", lambda ticker: fake_yf_ticker)

    updated = polygon_stock_service.update_stocks_in_db_from_polygon([{"symbol": "NOGROW"}])
    assert updated == 1

    with get_db() as db:
        row = db.query(Stock).filter(Stock.symbol == "NOGROW").first()
        assert row is not None
        assert row.annual_eps_growth_rate is None
        assert row.pe_per_growth is None
        assert row.revenue_per_share == Decimal("20.00")
        assert row.price_per_sales == Decimal("5.10")


def test_update_stocks_handles_missing_outstanding_shares_for_revenue_per_share(monkeypatch, fake_details):
    now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    yesterday = now - timedelta(days=1)
    bars = [
        SimpleNamespace(timestamp=_to_ms(yesterday), open=99, high=101, low=95, close=100, volume=1000),
        SimpleNamespace(timestamp=_to_ms(now), open=101, high=103, low=99, close=102, volume=1100),
    ]

    current_year = datetime.now().year
    reports = [
        _make_report(current_year, 1.0),
        _make_report(current_year - 1, 1.0),
    ]

    fake_client = _FakeClient(reports=reports, bars=bars, details=fake_details)

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(polygon_stock_service, "RESTClient", lambda api_key: fake_client)

    fake_yf_ticker = _FakeYfinanceTicker(
        shares_outstanding=None,
        trailing_eps=1.0,
        total_revenue=2_000_000_000,
        net_income_4q_ago=100_000_000
    )
    monkeypatch.setattr(polygon_stock_service.yf, "Ticker", lambda ticker: fake_yf_ticker)

    updated = polygon_stock_service.update_stocks_in_db_from_polygon([{"symbol": "NOSHARES"}])
    assert updated == 1

    with get_db() as db:
        row = db.query(Stock).filter(Stock.symbol == "NOSHARES").first()
        assert row is not None
        assert row.revenue_per_share is None
        assert row.price_per_sales is None

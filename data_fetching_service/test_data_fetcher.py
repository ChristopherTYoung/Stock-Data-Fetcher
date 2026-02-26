import pytest
from datetime import datetime, timedelta
from data_fetcher import DataFetcher
from database import get_db, StockHistory, Stock


def make_stock(symbol="TEST"):
    s = Stock(symbol=symbol, company_name="Test Co", updated_at=datetime.utcnow())
    return s


@pytest.fixture
def db_rows():
    # prepare three days of daily data
    now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    yesterday = now - timedelta(days=1)
    one_year_ago = now - timedelta(days=200)

    rows = [
        StockHistory(
            stock_symbol="TEST",
            day_and_time=one_year_ago,
            is_hourly=False,
            open_price=100,
            close_price=150,
            high=160,
            low=90,
            volume=1000,
        ),
        StockHistory(
            stock_symbol="TEST",
            day_and_time=yesterday - timedelta(hours=2),
            is_hourly=False,
            open_price=200,
            close_price=210,
            high=215,
            low=195,
            volume=2000,
        ),
        StockHistory(
            stock_symbol="TEST",
            day_and_time=now,
            is_hourly=False,
            open_price=220,
            close_price=225,
            high=230,
            low=215,
            volume=3000,
        ),
    ]
    return rows


def test_price_and_high_low52(db_rows):
    df = DataFetcher()

    # Insert rows into DB
    with get_db() as db:
        # ensure stock row exists
        db.add(make_stock())
        for r in db_rows:
            db.add(r)
        db.commit()

    # load stock object
    with get_db() as db:
        stock = db.query(Stock).filter(Stock.symbol == "TEST").first()

    # Calculate price (should be latest close_price)
    price = df.calculate("price", stock, None, None, {})
    assert price == 225

    # high52 should be max high in past year (we put 230)
    high52 = df.calculate("high52", stock, None, None, {})
    assert high52 == 230

    # low52 should be min low in past year (we put 90)
    low52 = df.calculate("low52", stock, None, None, {})
    assert low52 == 90


def test_percent_change(db_rows):
    df = DataFetcher()

    with get_db() as db:
        # Ensure stock exists
        db.add(make_stock())
        for r in db_rows:
            db.add(r)
        db.commit()

    with get_db() as db:
        stock = db.query(Stock).filter(Stock.symbol == "TEST").first()

    pct = df.calculate("percent_change", stock, None, None, {})
    # yesterday close_price was 210, current/latest is 225 -> ((225-210)/210)*100 = 7.142857...
    assert pytest.approx(pct, rel=1e-3) == 7.142857142857143

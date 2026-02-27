import pytest
from datetime import datetime, timedelta
from data_fetching_service.data_fetcher import DataFetcher
import data_fetching_service.data_fetcher as dfmod
import database as dbmod

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, BigInteger, Text, Numeric
from sqlalchemy.orm import sessionmaker, declarative_base
from contextlib import contextmanager


BaseTest = declarative_base()


class TestStockHistory(BaseTest):
    __tablename__ = "stock_history"
    stock_symbol = Column(String(20), primary_key=True, nullable=False)
    day_and_time = Column(DateTime, primary_key=True, nullable=False)
    is_hourly = Column(Boolean, primary_key=True, nullable=False)
    open_price = Column(Integer, nullable=False)
    close_price = Column(Integer, nullable=False)
    high = Column(Integer, nullable=False)
    low = Column(Integer, nullable=False)
    volume = Column(Integer, nullable=False)


class TestStock(BaseTest):
    __tablename__ = "stock"
    symbol = Column(String(10), primary_key=True)
    company_name = Column(String(100), nullable=False)
    updated_at = Column(DateTime, nullable=False)
    price = Column(Integer, nullable=True)


def make_stock(symbol="TEST"):
    return TestStock(symbol=symbol, company_name="Test Co", updated_at=datetime.utcnow())


@pytest.fixture
def db_rows():
    now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    yesterday = now - timedelta(days=1)
    one_year_ago = now - timedelta(days=200)

    rows = [
        TestStockHistory(
            stock_symbol="TEST",
            day_and_time=one_year_ago,
            is_hourly=False,
            open_price=100,
            close_price=150,
            high=160,
            low=90,
            volume=1000,
        ),
        TestStockHistory(
            stock_symbol="TEST",
            day_and_time=yesterday - timedelta(hours=2),
            is_hourly=False,
            open_price=200,
            close_price=210,
            high=215,
            low=195,
            volume=2000,
        ),
        TestStockHistory(
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


def _setup_in_memory(db_rows):
    engine = create_engine("sqlite:///:memory:")
    SessionLocal = sessionmaker(bind=engine)
    BaseTest.metadata.create_all(engine)

    # insert rows
    s = SessionLocal()
    s.add(make_stock())
    for r in db_rows:
        s.add(r)
    s.commit()
    s.close()

    # monkeypatch data_fetcher module to use test models and test session
    def fake_get_db():
        @contextmanager
        def _cm():
            session = SessionLocal()
            try:
                yield session
            finally:
                session.close()

        return _cm()

    # override references in the module under test
    dfmod.get_db = fake_get_db
    dfmod.Stock = TestStock
    dfmod.StockHistory = TestStockHistory


def test_price_and_high_low52(db_rows):
    _setup_in_memory(db_rows)
    df = DataFetcher()

    # load stock object from test DB
    with dfmod.get_db() as db:
        stock = db.query(dfmod.Stock).filter(dfmod.Stock.symbol == "TEST").first()

    price = df.calculate("price", stock, None, {})
    assert price == 225

    high52 = df.calculate("high52", stock, None, {})
    assert high52 == 230

    low52 = df.calculate("low52", stock, None, {})
    assert low52 == 90


def test_percent_change(db_rows):
    _setup_in_memory(db_rows)
    df = DataFetcher()

    with dfmod.get_db() as db:
        stock = db.query(dfmod.Stock).filter(dfmod.Stock.symbol == "TEST").first()

    pct = df.calculate("percent_change", stock, None, {})
    assert pytest.approx(pct, rel=1e-3) == 7.142857142857143

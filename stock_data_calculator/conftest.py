import os
import sys
import types
from contextlib import contextmanager

from sqlalchemy import Boolean, Column, DateTime, Integer, Numeric, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker


if "polygon" not in sys.modules:
    polygon_stub = types.ModuleType("polygon")

    class RESTClient:
        def __init__(self, *args, **kwargs):
            pass

    polygon_stub.RESTClient = RESTClient
    sys.modules["polygon"] = polygon_stub

BaseTest = declarative_base()


class Stock(BaseTest):
    __tablename__ = "stock"
    symbol = Column(String(10), primary_key=True)
    company_name = Column(String(100), nullable=False)
    updated_at = Column(DateTime, nullable=False)
    price = Column(Integer, nullable=True)
    high52 = Column(Integer, nullable=True)
    low52 = Column(Integer, nullable=True)
    percent_change = Column(Integer, nullable=True)
    eps = Column(Numeric(20, 6), nullable=True)
    revenue_per_share = Column(Numeric(20, 2), nullable=True)
    annual_eps_growth_rate = Column(Integer, nullable=True)
    price_per_sales = Column(Numeric(20, 2), nullable=True)
    price_per_earnings = Column(Integer, nullable=True)
    pe_per_growth = Column(Integer, nullable=True)
    debt_to_equity = Column(Numeric(20, 6), nullable=True)


class StockHistory(BaseTest):
    __tablename__ = "stock_history"
    stock_symbol = Column(String(20), primary_key=True, nullable=False)
    day_and_time = Column(DateTime, primary_key=True, nullable=False)
    is_hourly = Column(Boolean, primary_key=True, nullable=False)
    open_price = Column(Integer, nullable=False)
    close_price = Column(Integer, nullable=False)
    high = Column(Integer, nullable=False)
    low = Column(Integer, nullable=False)
    volume = Column(Integer, nullable=False)


engine = create_engine("sqlite:///:memory:")
SessionLocal = sessionmaker(bind=engine)
BaseTest.metadata.create_all(engine)


@contextmanager
def get_db():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db_session():
    return SessionLocal()


def close_db_connections():
    pass


def init_db():
    BaseTest.metadata.create_all(engine)


dbmod = types.ModuleType("stock_data_calculator.database")
dbmod.Base = BaseTest
dbmod.engine = engine
dbmod.SessionLocal = SessionLocal
dbmod.get_db = get_db
dbmod.Stock = Stock
dbmod.StockHistory = StockHistory
dbmod.get_db_session = get_db_session
dbmod.close_db_connections = close_db_connections
dbmod.init_db = init_db

# Register both names so direct and package imports resolve consistently.
sys.modules["stock_data_calculator.database"] = dbmod
sys.modules["database"] = dbmod

import os
import sys
import types
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, BigInteger, Text, Numeric
from sqlalchemy.orm import sessionmaker, declarative_base
from contextlib import contextmanager
import datetime

# Ensure tests import local package modules using package-style imports
pkg_dir = os.path.dirname(__file__)
if pkg_dir not in sys.path:
    sys.path.insert(0, pkg_dir)

# Provide a lightweight stub for the external `polygon` package so tests
# don't need the real client installed during collection.
if "polygon" not in sys.modules:
    polygon_stub = types.ModuleType("polygon")

    class RESTClient:
        def __init__(self, *args, **kwargs):
            pass

    polygon_stub.RESTClient = RESTClient
    sys.modules["polygon"] = polygon_stub

    # Inject an in-memory `database` module so tests don't hit Postgres.
    if "database" not in sys.modules:
        dbmod = types.ModuleType("database")
        BaseTest = declarative_base()

        class Stock(BaseTest):
            __tablename__ = "stock"
            symbol = Column(String(10), primary_key=True)
            company_name = Column(String(100), nullable=False)
            updated_at = Column(DateTime, nullable=False)
            price = Column(Integer, nullable=True)

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

        class Blacklist(BaseTest):
            __tablename__ = "blacklist"
            id = Column(Integer, primary_key=True, autoincrement=True)
            stock_symbol = Column(String(10), nullable=False)
            timestamp = Column(DateTime, nullable=False)
            time_added = Column(DateTime, nullable=False)
            is_hourly = Column(Boolean, default=True)

        engine = create_engine("sqlite:///:memory:")
        SessionLocal = sessionmaker(bind=engine)
        BaseTest.metadata.create_all(engine)

        @contextmanager
        def get_db():
            session = SessionLocal()
            try:
                yield session
                session.commit()
            except:
                session.rollback()
                raise
            finally:
                session.close()

        dbmod.Base = BaseTest
        dbmod.engine = engine
        dbmod.SessionLocal = SessionLocal
        dbmod.get_db = get_db
        dbmod.Stock = Stock
        dbmod.StockHistory = StockHistory
        dbmod.Blacklist = Blacklist

        def get_db_session():
            return SessionLocal()

        def close_db_connections():
            pass

        def init_db():
            BaseTest.metadata.create_all(engine)

        dbmod.get_db_session = get_db_session
        dbmod.close_db_connections = close_db_connections
        dbmod.init_db = init_db

        sys.modules["database"] = dbmod

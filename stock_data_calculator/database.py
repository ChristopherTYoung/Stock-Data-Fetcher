"""Database models and connection setup for stock_data_calculator."""

from contextlib import contextmanager
import logging
import os

from sqlalchemy import Boolean, BigInteger, Column, DateTime, Integer, Numeric, String, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from stock_data_calculator.logging_config import setup_logging

logger = setup_logging("stock-data-calculator", level=logging.INFO)

# Database URL from environment variable
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/stock_data",
)

# Create engine with schema search path and connection pooling
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
    connect_args={"options": "-csearch_path=incrementum,public"},
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


class StockHistory(Base):
    """Stock history table model matching the incrementum schema."""

    __tablename__ = "stock_history"
    __table_args__ = {"schema": "incrementum"}

    stock_symbol = Column("stock_symbol", String(20), primary_key=True, nullable=False)
    day_and_time = Column("day_and_time", DateTime, primary_key=True, nullable=False)
    is_hourly = Column("is_hourly", Boolean, primary_key=True, nullable=False, server_default="true")
    open_price = Column("open_price", Integer, nullable=False)
    close_price = Column("close_price", Integer, nullable=False)
    high = Column("high", Integer, nullable=False)
    low = Column("low", Integer, nullable=False)
    volume = Column("volume", Integer, nullable=False)

    def __repr__(self):
        return f"<StockHistory(symbol={self.stock_symbol}, time={self.day_and_time})>"


class Stock(Base):
    """Stock table model for storing stock symbols and calculated metrics."""

    __tablename__ = "stock"
    __table_args__ = {"schema": "incrementum"}

    symbol = Column("symbol", String(10), primary_key=True)
    company_name = Column("company_name", String(100), nullable=False)
    updated_at = Column("updated_at", DateTime, nullable=False)
    description = Column("description", Text, nullable=True)
    market_cap = Column("market_cap", BigInteger, nullable=True)
    primary_exchange = Column("primary_exchange", String(100), nullable=True)
    type = Column("type", String(50), nullable=True)
    currency_name = Column("currency_name", String(50), nullable=True)
    cik = Column("cik", String(50), nullable=True)
    composite_figi = Column("composite_figi", String(50), nullable=True)
    share_class_figi = Column("share_class_figi", String(50), nullable=True)
    outstanding_shares = Column("outstanding_shares", BigInteger, nullable=True)
    total_revenue = Column("total_revenue", BigInteger, nullable=True)
    eps = Column("eps", Numeric(20, 6), nullable=True)
    revenue_per_share = Column("revenue_per_share", Numeric(20, 2), nullable=True)
    homepage_url = Column("homepage_url", String(255), nullable=True)
    total_employees = Column("total_employees", Integer, nullable=True)
    list_date = Column("list_date", DateTime, nullable=True)
    locale = Column("locale", String(20), nullable=True)
    sic_code = Column("sic_code", String(20), nullable=True)
    sic_description = Column("sic_description", String(255), nullable=True)
    price = Column("price", Integer, nullable=True)
    high52 = Column("high52", Integer, nullable=True)
    low52 = Column("low52", Integer, nullable=True)
    percent_change = Column("percent_change", Integer, nullable=True)
    annual_eps_growth_rate = Column("annual_eps_growth_rate", Integer, nullable=True)
    price_per_sales = Column("price_per_sales", Numeric(20, 2), nullable=True)
    price_per_earnings = Column("price_per_earnings", Integer, nullable=True)
    pe_per_growth = Column("pe_per_growth", Integer, nullable=True)
    high52_updated_at = Column("high52_updated_at", DateTime, nullable=True)
    low52_updated_at = Column("low52_updated_at", DateTime, nullable=True)
    quarterly_financials_updated_at = Column("quarterly_financials_updated_at", DateTime, nullable=True)
    debt_to_equity = Column("debt_to_equity", Numeric(20, 6), nullable=True)

    def __repr__(self):
        return f"<Stock(symbol={self.symbol}, company={self.company_name})>"


class Blacklist(Base):
    """Blacklist table model for storing blacklisted stock gaps."""

    __tablename__ = "blacklist"
    __table_args__ = {"schema": "incrementum"}

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    stock_symbol = Column("stock_symbol", String(10), nullable=False)
    timestamp = Column("timestamp", DateTime, nullable=False)
    time_added = Column("time_added", DateTime, nullable=False)
    is_hourly = Column("is_hourly", Boolean, default=True)

    def __repr__(self):
        return (
            f"<Blacklist(symbol={self.stock_symbol}, timestamp={self.timestamp}, "
            f"added={self.time_added}, is_hourly={self.is_hourly})>"
        )


def init_db():
    """Create all tables in the database."""
    try:
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        raise


@contextmanager
def get_db():
    """Get database session with automatic cleanup."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_db_session():
    """Get a new database session."""
    return SessionLocal()


def close_db_connections():
    """Close all database connections and dispose of the engine."""
    try:
        engine.dispose()
        logger.info("Database connections closed successfully")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

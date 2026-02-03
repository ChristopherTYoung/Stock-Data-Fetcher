"""
Database models and connection setup for stock data storage.
"""
from sqlalchemy import create_engine, Column, Integer, String, DateTime, BigInteger, Boolean, Text, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Database URL from environment variable
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/stock_data"
)

# Create engine with schema search path
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True,
    connect_args={"options": "-csearch_path=incrementum,public"}
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


class StockHistory(Base):
    """Stock history table model matching the incrementum schema."""
    __tablename__ = "stock_history"
    __table_args__ = {'schema': 'incrementum'}
    
    stock_symbol = Column("stock_symbol", String(20), primary_key=True, nullable=False)
    day_and_time = Column("day_and_time", DateTime, primary_key=True, nullable=False)
    is_hourly = Column("is_hourly", Boolean, primary_key=True, nullable=False, server_default='true')
    open_price = Column("open_price", Integer, nullable=False)
    close_price = Column("close_price", Integer, nullable=False)
    high = Column("high", Integer, nullable=False)
    low = Column("low", Integer, nullable=False)
    volume = Column("volume", Integer, nullable=False)
    
    def __repr__(self):
        return f"<StockHistory(symbol={self.stock_symbol}, time={self.day_and_time})>"


class Stock(Base):
    """Stock table model for storing stock symbols."""
    __tablename__ = "stock"
    __table_args__ = {'schema': 'incrementum'}
    
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
    eps = Column("eps", Numeric(20, 6), nullable=True)
    homepage_url = Column("homepage_url", String(255), nullable=True)
    total_employees = Column("total_employees", Integer, nullable=True)
    list_date = Column("list_date", DateTime, nullable=True)
    locale = Column("locale", String(20), nullable=True)
    sic_code = Column("sic_code", String(20), nullable=True)
    sic_description = Column("sic_description", String(255), nullable=True)
    
    def __repr__(self):
        return f"<Stock(symbol={self.symbol}, company={self.company_name})>"


class Blacklist(Base):
    """Blacklist table model for storing blacklisted stock gaps."""
    __tablename__ = "blacklist"
    __table_args__ = {'schema': 'incrementum'}
    
    id = Column("id", Integer, primary_key=True, autoincrement=True)
    stock_symbol = Column("stock_symbol", String(10), nullable=False)
    timestamp = Column("timestamp", DateTime, nullable=False)
    time_added = Column("time_added", DateTime, nullable=False)
    is_hourly = Column("is_hourly", Boolean, default=True)
    
    def __repr__(self):
        return f"<Blacklist(symbol={self.stock_symbol}, timestamp={self.timestamp}, added={self.time_added}, is_hourly={self.is_hourly})>"


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

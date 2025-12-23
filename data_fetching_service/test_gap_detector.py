"""
Tests for the gap detector module.
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from gap_detector import GapDetector

# Create test-specific models without schema for SQLite compatibility
TestBase = declarative_base()


class TestStock(TestBase):
    """Test stock table model without schema."""
    __tablename__ = "stock"
    
    symbol = Column("symbol", String(10), primary_key=True)
    company_name = Column("company_name", String(100), nullable=False)
    updated_at = Column("updated_at", DateTime, nullable=False)


class TestStockHistory(TestBase):
    """Test stock history table model without schema."""
    __tablename__ = "stock_history"
    
    stock_symbol = Column("stock_symbol", String(20), primary_key=True, nullable=False)
    day_and_time = Column("day_and_time", DateTime, primary_key=True, nullable=False)
    is_hourly = Column("is_hourly", Boolean, primary_key=True, nullable=False, server_default='true')
    open_price = Column("open_price", Integer, nullable=False)
    close_price = Column("close_price", Integer, nullable=False)
    high = Column("high", Integer, nullable=False)
    low = Column("low", Integer, nullable=False)
    volume = Column("volume", Integer, nullable=False)


class TestBlacklist(TestBase):
    """Test blacklist table model without schema."""
    __tablename__ = "blacklist"
    
    id = Column("id", Integer, primary_key=True, autoincrement=True)
    stock_symbol = Column("stock_symbol", String(10), nullable=False)
    timestamp = Column("timestamp", DateTime, nullable=False)
    time_added = Column("time_added", DateTime, nullable=False)


# Create an in-memory SQLite database for testing
@pytest.fixture
def test_db():
    """Create a test database without schema for SQLite compatibility."""
    engine = create_engine("sqlite:///:memory:")
    TestBase.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    yield session
    
    session.close()
    engine.dispose()


def setup_gap_detector_for_test(gap_detector, test_db, monkeypatch):
    """Helper function to configure gap detector for testing."""
    from contextlib import contextmanager
    
    @contextmanager
    def mock_get_db():
        yield test_db
    
    monkeypatch.setattr("gap_detector.get_db", mock_get_db)
    
    # Set model references on the gap_detector instance
    gap_detector.Stock = TestStock
    gap_detector.StockHistory = TestStockHistory
    gap_detector.Blacklist = TestBlacklist


@pytest.fixture
def gap_detector():
    """Create a gap detector instance."""
    return GapDetector(blacklist_expiration_time=24)


@pytest.fixture
def sample_stock(test_db):
    """Create a sample stock in the database."""
    stock = TestStock(
        symbol="AAPL",
        company_name="Apple Inc.",
        updated_at=datetime.now()
    )
    test_db.add(stock)
    test_db.commit()
    return stock


def test_no_hourly_data_exists(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when no hourly data exists for a stock (entire 2 years is a gap)."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find one gap covering 2 years of hourly data
    hourly_gaps = [g for g in gaps if g[2] == True]
    assert len(hourly_gaps) == 1
    
    gap_start, gap_end, is_hourly = hourly_gaps[0]
    assert is_hourly == True
    assert (gap_end - gap_start).days >= 729  # Approximately 2 years


def test_no_minute_data_exists(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when no minute data exists for a stock (entire 30 days is a gap)."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    # Add some hourly data so we can isolate minute gaps
    now = datetime.now()
    for i in range(100):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find minute data gap
    minute_gaps = [g for g in gaps if g[2] == False]
    assert len(minute_gaps) == 1
    
    gap_start, gap_end, is_hourly = minute_gaps[0]
    assert is_hourly == False
    assert (gap_end - gap_start).days >= 29  # Approximately 30 days


def test_gap_between_consecutive_hourly_points(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when there's a gap between consecutive hourly data points."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    now = datetime.now()
    
    # Add hourly data with a 10-day gap in the middle
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Create a gap - skip 10 days
    gap_start = now - timedelta(hours=50)
    gap_end = gap_start - timedelta(days=10)
    
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=gap_end - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find the gap between consecutive points
    hourly_gaps = [g for g in gaps if g[2] == True]
    assert len(hourly_gaps) >= 1
    
    # Check that at least one gap is around 10 days
    large_gaps = [g for g in hourly_gaps if (g[1] - g[0]).days >= 9]
    assert len(large_gaps) >= 1


def test_gap_between_consecutive_minute_points(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when there's a gap between consecutive minute data points."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    now = datetime.now()
    
    # Add minute data with a 2-day gap in the middle
    for i in range(500):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(minutes=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=False
        )
        test_db.add(record)
    
    # Create a gap - skip 2 days
    gap_start = now - timedelta(minutes=500)
    gap_end = gap_start - timedelta(days=2)
    
    for i in range(500):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=gap_end - timedelta(minutes=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=False
        )
        test_db.add(record)
    
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find the gap between consecutive minute points
    minute_gaps = [g for g in gaps if g[2] == False]
    assert len(minute_gaps) >= 1
    
    # Check that at least one gap is around 2 days
    large_gaps = [g for g in minute_gaps if (g[1] - g[0]).days >= 1]
    assert len(large_gaps) >= 1


def test_hourly_data_doesnt_reach_2_years(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when hourly data exists but doesn't go back 2 years."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    now = datetime.now()
    
    # Add hourly data for only 1 year (not 2)
    for i in range(365 * 24):  # 1 year of hourly data
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find a gap for the missing year
    hourly_gaps = [g for g in gaps if g[2] == True]
    assert len(hourly_gaps) >= 1
    
    # Check that there's a historical gap
    oldest_data = now - timedelta(hours=365 * 24)
    two_years_ago = now - timedelta(days=730)
    historical_gaps = [g for g in hourly_gaps if g[0] < oldest_data]
    assert len(historical_gaps) >= 1


def test_minute_data_doesnt_reach_30_days(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when minute data exists but doesn't go back 30 days."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    now = datetime.now()
    
    # Add minute data for only 15 days (not 30)
    for i in range(15 * 24 * 60):  # 15 days of minute data
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(minutes=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=False
        )
        test_db.add(record)
    
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find a gap for the missing 15 days
    minute_gaps = [g for g in gaps if g[2] == False]
    assert len(minute_gaps) >= 1
    
    # Check that there's a historical gap
    oldest_data = now - timedelta(minutes=15 * 24 * 60)
    thirty_days_ago = now - timedelta(days=30)
    historical_gaps = [g for g in minute_gaps if g[0] < oldest_data]
    assert len(historical_gaps) >= 1


def test_missing_recent_hourly_data(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when hourly data exists but is outdated (not recent)."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    # Add hourly data that stops 10 days ago
    ten_days_ago = datetime.now() - timedelta(days=10)
    
    for i in range(500):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=ten_days_ago - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find a gap for recent data
    hourly_gaps = [g for g in gaps if g[2] == True]
    assert len(hourly_gaps) >= 1
    
    # Check that there's a recent gap (gap end should be close to now)
    recent_gaps = [g for g in hourly_gaps if (datetime.now() - g[1]).days < 1]
    assert len(recent_gaps) >= 1


def test_missing_recent_minute_data(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when minute data exists but is outdated (not recent)."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    # Add minute data that stops 3 days ago
    three_days_ago = datetime.now() - timedelta(days=3)
    
    for i in range(2000):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=three_days_ago - timedelta(minutes=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=False
        )
        test_db.add(record)
    
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find a gap for recent data
    minute_gaps = [g for g in gaps if g[2] == False]
    assert len(minute_gaps) >= 1
    
    # Check that there's a recent gap (gap end should be close to now)
    recent_gaps = [g for g in minute_gaps if (datetime.now() - g[1]).days < 1]
    assert len(recent_gaps) >= 1


def test_stock_not_found(test_db, gap_detector, monkeypatch):
    """Test when stock doesn't exist in database."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    gaps = gap_detector.check_for_gaps("INVALID")
    
    # Should return empty list when stock doesn't exist
    assert gaps == []


def test_complete_data_no_gaps(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when data is complete with no gaps."""
    # Fix the current time to ensure consistency
    fixed_now = datetime.now()
    
    # Create a mock datetime module that returns fixed_now
    from unittest.mock import Mock
    mock_datetime = Mock()
    mock_datetime.now = Mock(return_value=fixed_now)
    
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    monkeypatch.setattr("gap_detector.datetime", mock_datetime)
    
    # Add complete hourly data for 2 years + a bit extra, starting from now
    # We add extra to ensure we go beyond the 2-year boundary
    for i in range((2 * 365 * 24) + 200):  # 2 years + extra buffer
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=fixed_now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Commit hourly data
    test_db.commit()
    
    # Add complete minute data for 30 days + a bit extra, starting from now
    # We add extra to ensure we go beyond the 30-day boundary
    for i in range((30 * 24 * 60) + 2000):  # 30 days + extra buffer
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=fixed_now - timedelta(minutes=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=False
        )
        test_db.add(record)
    
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find no gaps - we have continuous hourly data for 2 years 
    # and minute data for 30 days, both up to the fixed "now"
    assert len(gaps) == 0


def test_multiple_gaps_in_hourly_data(test_db, gap_detector, sample_stock, monkeypatch):
    """Test when there are multiple gaps in hourly data."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    now = datetime.now()
    
    # Add hourly data in chunks with gaps between them
    # Chunk 1: Recent data
    for i in range(100):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Gap 1: 15 days
    
    # Chunk 2: Middle data
    chunk2_start = now - timedelta(hours=100) - timedelta(days=15)
    for i in range(100):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=chunk2_start - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Gap 2: 20 days
    
    # Chunk 3: Older data
    chunk3_start = chunk2_start - timedelta(hours=100) - timedelta(days=20)
    for i in range(100):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=chunk3_start - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Should find multiple gaps
    hourly_gaps = [g for g in gaps if g[2] == True]
    assert len(hourly_gaps) >= 2
    
    # Check that gaps are significant (> 7 days)
    significant_gaps = [g for g in hourly_gaps if (g[1] - g[0]).days > 7]
    assert len(significant_gaps) >= 2


def test_blacklist_expiration_time_initialization(gap_detector):
    """Test that blacklist expiration time is properly initialized."""
    assert gap_detector.blacklist_expiration_time == 24
    
    # Test with different value
    detector2 = GapDetector(blacklist_expiration_time=48)
    assert detector2.blacklist_expiration_time == 48


def test_blacklist_filters_active_gaps(test_db, gap_detector, sample_stock, monkeypatch):
    """Test that gaps in active blacklist are filtered out."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    now = datetime.now()
    
    # Add hourly data with a 10-day gap
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Create a gap - skip 10 days
    gap_start_time = now - timedelta(hours=50)
    gap_end_time = gap_start_time - timedelta(days=10)
    
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=gap_end_time - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Add blacklist entry for this gap (added recently, within expiration time)
    # The actual gap starts at gap_end_time (the last point of older data)
    blacklist_entry = TestBlacklist(
        stock_symbol="AAPL",
        timestamp=gap_end_time,
        time_added=now - timedelta(hours=1)  # Added 1 hour ago
    )
    test_db.add(blacklist_entry)
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # The 10-day gap should be filtered out since it's in active blacklist
    # We should NOT find a gap around the 10-day range we created
    hourly_gaps = [g for g in gaps if g[2] == True]
    
    # Check that we don't have the specific 10-day gap we created and blacklisted
    # The gap would be approximately 10 days (between 9 and 11 days)
    ten_day_gaps = [g for g in hourly_gaps if 9 <= (g[1] - g[0]).days <= 11]
    assert len(ten_day_gaps) == 0, "The 10-day gap should be filtered by blacklist"


def test_blacklist_returns_expired_gaps(test_db, gap_detector, sample_stock, monkeypatch):
    """Test that gaps with expired blacklist entries are returned."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    now = datetime.now()
    
    # Add hourly data with a 10-day gap
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Create a gap - skip 10 days
    gap_start_time = now - timedelta(hours=50)
    gap_end_time = gap_start_time - timedelta(days=10)
    
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=gap_end_time - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Add blacklist entry that has expired (added more than 24 hours ago)
    # The actual gap starts at gap_end_time
    blacklist_entry = TestBlacklist(
        stock_symbol="AAPL",
        timestamp=gap_end_time,
        time_added=now - timedelta(hours=48)  # Added 48 hours ago, expired
    )
    test_db.add(blacklist_entry)
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # The gap should be returned since the blacklist entry has expired
    hourly_gaps = [g for g in gaps if g[2] == True]
    large_gaps = [g for g in hourly_gaps if (g[1] - g[0]).days >= 9]
    assert len(large_gaps) >= 1, "Gap should be returned when blacklist expires"


def test_blacklist_does_not_filter_different_symbol(test_db, gap_detector, monkeypatch):
    """Test that blacklist entries for one symbol don't affect another symbol."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    # Create two stocks
    stock1 = TestStock(symbol="AAPL", company_name="Apple Inc.", updated_at=datetime.now())
    stock2 = TestStock(symbol="GOOGL", company_name="Google LLC", updated_at=datetime.now())
    test_db.add(stock1)
    test_db.add(stock2)
    test_db.commit()
    
    now = datetime.now()
    
    # Add hourly data with a 10-day gap for GOOGL
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="GOOGL",
            day_and_time=now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Create a gap - skip 10 days
    gap_start_time = now - timedelta(hours=50)
    gap_end_time = gap_start_time - timedelta(days=10)
    
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="GOOGL",
            day_and_time=gap_end_time - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Add blacklist entry for AAPL (different symbol)
    # Use gap_end_time which is where the gap actually starts
    blacklist_entry = TestBlacklist(
        stock_symbol="AAPL",
        timestamp=gap_end_time,
        time_added=now - timedelta(hours=1)
    )
    test_db.add(blacklist_entry)
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("GOOGL")
    
    # The gap should still be found since blacklist is for different symbol
    hourly_gaps = [g for g in gaps if g[2] == True]
    large_gaps = [g for g in hourly_gaps if (g[1] - g[0]).days >= 9]
    assert len(large_gaps) >= 1, "Gap should not be filtered by blacklist for different symbol"


def test_blacklist_filters_multiple_gaps(test_db, gap_detector, sample_stock, monkeypatch):
    """Test that multiple blacklist entries can filter multiple gaps."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    now = datetime.now()
    
    # Add hourly data in chunks with gaps between them
    # Chunk 1: Recent data
    for i in range(100):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Gap 1: 15 days
    gap1_start = now - timedelta(hours=100)
    gap1_end = gap1_start - timedelta(days=15)
    
    # Chunk 2: Middle data
    for i in range(100):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=gap1_end - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Gap 2: 20 days
    gap2_start = gap1_end - timedelta(hours=100)
    gap2_end = gap2_start - timedelta(days=20)
    
    # Chunk 3: Older data
    for i in range(100):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=gap2_end - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Add blacklist entries for both gaps
    # Gaps start at gap1_end and gap2_end respectively
    blacklist1 = TestBlacklist(
        stock_symbol="AAPL",
        timestamp=gap1_end,
        time_added=now - timedelta(hours=1)
    )
    blacklist2 = TestBlacklist(
        stock_symbol="AAPL",
        timestamp=gap2_end,
        time_added=now - timedelta(hours=2)
    )
    test_db.add(blacklist1)
    test_db.add(blacklist2)
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # Both created gaps should be filtered out
    # We should NOT find the 15-day and 20-day gaps we created and blacklisted
    hourly_gaps = [g for g in gaps if g[2] == True]
    
    # Check for the specific gaps we created (15 days and 20 days, allowing some tolerance)
    created_gaps = [g for g in hourly_gaps if 10 <= (g[1] - g[0]).days <= 25]
    assert len(created_gaps) == 0, "All blacklisted gaps should be filtered"


def test_blacklist_no_entries(test_db, gap_detector, sample_stock, monkeypatch):
    """Test that gaps are returned normally when blacklist is empty."""
    setup_gap_detector_for_test(gap_detector, test_db, monkeypatch)
    
    now = datetime.now()
    
    # Add hourly data with a 10-day gap
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=now - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    # Create a gap - skip 10 days
    gap_start_time = now - timedelta(hours=50)
    gap_end_time = gap_start_time - timedelta(days=10)
    
    for i in range(50):
        record = TestStockHistory(
            stock_symbol="AAPL",
            day_and_time=gap_end_time - timedelta(hours=i),
            open_price=15000,
            close_price=15100,
            high=15200,
            low=14900,
            volume=1000000,
            is_hourly=True
        )
        test_db.add(record)
    
    test_db.commit()
    
    gaps = gap_detector.check_for_gaps("AAPL")
    
    # The gap should be found since there's no blacklist entry
    hourly_gaps = [g for g in gaps if g[2] == True]
    large_gaps = [g for g in hourly_gaps if (g[1] - g[0]).days >= 9]
    assert len(large_gaps) >= 1, "Gap should be returned when no blacklist entries exist"

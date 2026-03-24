"""Comprehensive tests for StockCalculator to verify calculations aren't returning None unexpectedly."""
import pytest
from datetime import datetime, timedelta
from decimal import Decimal
import pandas as pd
from stock_data_calculator.stock_calculator import StockCalculator
import stock_data_calculator.database as dbmod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager


BaseTest = dbmod.Base


class TestStockHistory(BaseTest):
    """Test model for stock_history."""
    __tablename__ = "stock_history"
    stock_symbol = dbmod.StockHistory.stock_symbol
    day_and_time = dbmod.StockHistory.day_and_time
    is_hourly = dbmod.StockHistory.is_hourly
    open_price = dbmod.StockHistory.open_price
    close_price = dbmod.StockHistory.close_price
    high = dbmod.StockHistory.high
    low = dbmod.StockHistory.low
    volume = dbmod.StockHistory.volume


class TestStock(BaseTest):
    """Test model for stock."""
    __tablename__ = "stock"
    symbol = dbmod.Stock.symbol
    company_name = dbmod.Stock.company_name
    updated_at = dbmod.Stock.updated_at
    price = dbmod.Stock.price


@pytest.fixture
def simple_dataframe():
    """Create a simple DataFrame with price data."""
    now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    data = {
        'open': [100, 105, 110],
        'high': [102, 107, 112],
        'low': [99, 104, 109],
        'close': [101, 106, 111],
        'volume': [1000, 1100, 1200]
    }
    df = pd.DataFrame(data, index=pd.DatetimeIndex([
        now - timedelta(days=2),
        now - timedelta(days=1),
        now
    ], name='timestamp'))
    return df


@pytest.fixture
def one_year_dataframe():
    """Create a DataFrame with data spanning ~1 year."""
    now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    dates = [now - timedelta(days=i) for i in range(0, 365, 5)]  # Every 5 days
    data = {
        'open': list(range(100, 100 + len(dates))),
        'high': list(range(102, 102 + len(dates))),
        'low': list(range(98, 98 + len(dates))),
        'close': list(range(101, 101 + len(dates))),
        'volume': [1000] * len(dates)
    }
    df = pd.DataFrame(data, index=pd.DatetimeIndex(dates, name='timestamp'))
    df = df.sort_index()
    return df


@pytest.fixture
def test_stock():
    """Create a test stock object with symbol."""
    stock = TestStock(symbol="TEST", company_name="Test Corp", updated_at=datetime.utcnow())
    return stock


class TestCalculatePrice:
    """Test price calculation."""
    
    def test_price_from_dataframe(self, simple_dataframe):
        """Price should be last close from DataFrame."""
        result = StockCalculator.calculate_price(simple_dataframe, None)
        assert result is not None, "Price calculation returned None for valid DataFrame"
        assert result == 111, f"Expected 111, got {result}"
    
    def test_price_empty_dataframe(self):
        """Should return None for empty DataFrame."""
        result = StockCalculator.calculate_price(pd.DataFrame(), None)
        assert result is None, "Price should be None for empty DataFrame"
    
    def test_price_none_dataframe(self):
        """Should return None when DataFrame is None."""
        result = StockCalculator.calculate_price(None, None)
        assert result is None, "Price should be None when DataFrame is None"


class TestCalculateHigh52:
    """Test 52-week high calculation."""
    
    def test_high52_from_dataframe(self, one_year_dataframe, test_stock):
        """Should calculate 52-week high from DataFrame."""
        result = StockCalculator.calculate_high52(one_year_dataframe, test_stock)
        assert result is not None, "High52 calculation returned None for valid DataFrame"
        expected = one_year_dataframe['high'].max()
        assert result == expected, f"Expected {expected}, got {result}"
    
    def test_high52_empty_dataframe(self, test_stock):
        """Should return None for empty DataFrame."""
        result = StockCalculator.calculate_high52(pd.DataFrame(), test_stock)
        assert result is None, "High52 should be None for empty DataFrame"
    
    def test_high52_none_dataframe(self, test_stock):
        """Should return None when DataFrame is None."""
        result = StockCalculator.calculate_high52(None, test_stock)
        assert result is None, "High52 should be None when DataFrame is None"
    
    def test_high52_with_stock(self, one_year_dataframe, test_stock):
        """Should calculate with stock object containing symbol."""
        result = StockCalculator.calculate_high52(one_year_dataframe, test_stock)
        assert result is not None, "High52 should calculate with stock object"


class TestCalculateLow52:
    """Test 52-week low calculation."""
    
    def test_low52_from_dataframe(self, one_year_dataframe, test_stock):
        """Should calculate 52-week low from DataFrame."""
        result = StockCalculator.calculate_low52(one_year_dataframe, test_stock)
        assert result is not None, "Low52 calculation returned None for valid DataFrame"
        expected = one_year_dataframe['low'].min()
        assert result == expected, f"Expected {expected}, got {result}"
    
    def test_low52_empty_dataframe(self, test_stock):
        """Should return None for empty DataFrame."""
        result = StockCalculator.calculate_low52(pd.DataFrame(), test_stock)
        assert result is None, "Low52 should be None for empty DataFrame"
    
    def test_low52_none_dataframe(self, test_stock):
        """Should return None when DataFrame is None."""
        result = StockCalculator.calculate_low52(None, test_stock)
        assert result is None, "Low52 should be None when DataFrame is None"


class TestCalculatePercentChange:
    """Test percent change calculation."""
    
    def test_percent_change_from_dataframe(self, test_stock):
        """Should calculate percent change from yesterday to today."""
        now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
        yesterday = now - timedelta(days=1)
        
        data = {
            'open': [100, 100],
            'high': [102, 110],
            'low': [99, 99],
            'close': [101, 110],  # 9% increase
            'volume': [1000, 1000]
        }
        df = pd.DataFrame(data, index=pd.DatetimeIndex([yesterday, now], name='timestamp'))
        
        result = StockCalculator.calculate_percent_change(df, test_stock)
        assert result is not None, "Percent change should be calculated for multi-day data"
        expected = ((110 - 101) / 101) * 100
        assert abs(result - expected) < 0.01, f"Expected {expected}, got {result}"
    
    def test_percent_change_empty_dataframe(self, test_stock):
        """Should return None for empty DataFrame."""
        result = StockCalculator.calculate_percent_change(pd.DataFrame(), test_stock)
        assert result is None, "Percent change should be None for empty DataFrame"
    
    def test_percent_change_none_dataframe(self, test_stock):
        """Should return None when DataFrame is None."""
        result = StockCalculator.calculate_percent_change(None, test_stock)
        assert result is None, "Percent change should be None when DataFrame is None"
    
    def test_percent_change_zero_yesterday_close(self, test_stock):
        """Should return None when yesterday's close is 0."""
        now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
        yesterday = now - timedelta(days=1)
        
        data = {
            'open': [0, 100],
            'high': [0, 110],
            'low': [0, 99],
            'close': [0, 110],
            'volume': [0, 1000]
        }
        df = pd.DataFrame(data, index=pd.DatetimeIndex([yesterday, now], name='timestamp'))
        
        result = StockCalculator.calculate_percent_change(df, test_stock)
        assert result is None, "Percent change should be None when yesterday's close is 0"


class TestCalculationsWithRealScenarios:
    """Test realistic scenarios that might occur in production."""
    
    def test_fresh_stock_no_data(self, test_stock):
        """New stock with no historical data should return None."""
        price = StockCalculator.calculate_price(None, test_stock)
        assert price is None, "Price should be None for stock with no data"
        
        high52 = StockCalculator.calculate_high52(None, test_stock)
        assert high52 is None, "High52 should be None for stock with no data"
        
        low52 = StockCalculator.calculate_low52(None, test_stock)
        assert low52 is None, "Low52 should be None for stock with no data"
        
        pct = StockCalculator.calculate_percent_change(None, test_stock)
        assert pct is None, "Percent change should be None for stock with no data"
    
    def test_stock_with_only_today_data(self, test_stock):
        """Stock with only today's data."""
        now = datetime.utcnow()
        data = {
            'open': [100],
            'high': [110],
            'low': [95],
            'close': [105],
            'volume': [1000]
        }
        df = pd.DataFrame(data, index=pd.DatetimeIndex([now], name='timestamp'))
        
        price = StockCalculator.calculate_price(df, test_stock)
        assert price == 105, "Price should be 105"
        
        high52 = StockCalculator.calculate_high52(df, test_stock)
        assert high52 == 110, "High52 should be 110"
        
        low52 = StockCalculator.calculate_low52(df, test_stock)
        assert low52 == 95, "Low52 should be 95"
        
        # Percent change needs yesterday's data
        pct = StockCalculator.calculate_percent_change(df, test_stock)
        assert pct is None, "Percent change should be None with single day of data"
    
    def test_dataframe_with_missing_columns(self, test_stock):
        """DataFrame missing required columns should handle gracefully."""
        now = datetime.utcnow()
        # Missing 'close' column
        data = {'open': [100], 'high': [110], 'low': [95], 'volume': [1000]}
        df = pd.DataFrame(data, index=pd.DatetimeIndex([now], name='timestamp'))
        
        # Should return None, not raise error
        result = StockCalculator.calculate_price(df, test_stock)
        assert result is None, "Price should return None for DataFrame missing close column"
    
    def test_dataframe_with_timestamp_column(self, test_stock):
        """DataFrame with 'timestamp' column (not index) should be handled."""
        now = datetime.utcnow()
        data = {
            'timestamp': [now],
            'open': [100],
            'high': [110],
            'low': [95],
            'close': [105],
            'volume': [1000]
        }
        df = pd.DataFrame(data)
        
        price = StockCalculator.calculate_price(df, test_stock)
        assert price == 105, "Price should be 105 even with timestamp as column"
    
    def test_percent_change_within_same_day(self, test_stock):
        """Percent change calculation for data within the same day."""
        now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
        morning = now.replace(hour=9, minute=30)
        afternoon = now.replace(hour=15, minute=0)
        
        data = {
            'open': [100, 105],
            'high': [102, 110],
            'low': [99, 104],
            'close': [101, 109],
            'volume': [1000, 1100]
        }
        df = pd.DataFrame(data, index=pd.DatetimeIndex([morning, afternoon], name='timestamp'))
        
        # Both are same day, so no yesterday data
        result = StockCalculator.calculate_percent_change(df, test_stock)
        assert result is None, "Percent change should be None when all data is same day"


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_negative_prices(self, test_stock):
        """Handle negative prices gracefully (shouldn't happen but test anyway)."""
        now = datetime.utcnow()
        data = {
            'open': [-100],
            'high': [-90],
            'low': [-110],
            'close': [-95],
            'volume': [1000]
        }
        df = pd.DataFrame(data, index=pd.DatetimeIndex([now], name='timestamp'))
        
        price = StockCalculator.calculate_price(df, test_stock)
        assert price == -95, "Should return negative price as-is"
    
    def test_nan_values(self, test_stock):
        """Handle NaN values in DataFrame."""
        now = datetime.utcnow()
        data = {
            'open': [100],
            'high': [110],
            'low': [95],
            'close': [float('nan')],
            'volume': [1000]
        }
        df = pd.DataFrame(data, index=pd.DatetimeIndex([now], name='timestamp'))
        
        result = StockCalculator.calculate_price(df, test_stock)
        # Should handle NaN without crashing
        assert pd.isna(result) or result is None, "Should handle NaN gracefully"
    
    def test_very_large_numbers(self, test_stock):
        """Handle very large numbers."""
        now = datetime.utcnow()
        data = {
            'open': [1000000],
            'high': [1100000],
            'low': [950000],
            'close': [1050000],
            'volume': [1000000]
        }
        df = pd.DataFrame(data, index=pd.DatetimeIndex([now], name='timestamp'))
        
        price = StockCalculator.calculate_price(df, test_stock)
        assert price == 1050000, "Should handle large numbers correctly"


class TestCalculatePE:
    """Test P/E ratio calculation."""
    
    def test_pe_with_valid_values(self, test_stock):
        """Calculate P/E with valid price and EPS."""
        test_stock.price = 100
        test_stock.eps = 5
        
        result = StockCalculator.calculate_pe(test_stock)
        assert result is not None, "P/E should be calculated for valid stock"
        assert result == 20.0, f"Expected P/E of 20, got {result}"
    
    def test_pe_with_price_parameter(self, test_stock):
        """P/E should use price parameter if provided."""
        test_stock.price = 100
        test_stock.eps = 4
        
        result = StockCalculator.calculate_pe(test_stock, price=200)
        assert result is not None, "P/E should be calculated with price parameter"
        assert result == 50.0, f"Expected P/E of 50 (200/4), got {result}"
    
    def test_pe_prefers_parameter_over_stock_price(self, test_stock):
        """Price parameter should take precedence over stock.price."""
        test_stock.price = 50
        test_stock.eps = 2
        
        result = StockCalculator.calculate_pe(test_stock, price=100)
        assert result == 50.0, f"Expected P/E of 50 (100/2), got {result}"
    
    def test_pe_no_stock(self):
        """Should return None when no stock object provided."""
        result = StockCalculator.calculate_pe(None)
        assert result is None, "P/E should be None for None stock"
    
    def test_pe_missing_price(self, test_stock):
        """Should return None when price is not available."""
        test_stock.price = None
        test_stock.eps = 5
        
        result = StockCalculator.calculate_pe(test_stock)
        assert result is None, "P/E should be None when price is missing"
    
    def test_pe_missing_eps(self, test_stock):
        """Should return None when EPS is not available."""
        test_stock.price = 100
        test_stock.eps = None
        
        result = StockCalculator.calculate_pe(test_stock)
        assert result is None, "P/E should be None when EPS is missing"
    
    def test_pe_zero_eps(self, test_stock):
        """Should return None when EPS is zero (division by zero)."""
        test_stock.price = 100
        test_stock.eps = 0
        
        result = StockCalculator.calculate_pe(test_stock)
        assert result is None, "P/E should be None when EPS is zero"
    
    def test_pe_fractional_values(self, test_stock):
        """Calculate P/E with fractional price and EPS."""
        test_stock.price = 150.50
        test_stock.eps = 2.75
        
        result = StockCalculator.calculate_pe(test_stock)
        assert result is not None, "P/E should be calculated with fractional values"
        expected = 150.50 / 2.75
        assert abs(result - expected) < 0.01, f"Expected P/E of {expected:.2f}, got {result:.2f}"
    
    def test_pe_high_pe_ratio(self, test_stock):
        """Handle high P/E ratios."""
        test_stock.price = 500
        test_stock.eps = 0.5
        
        result = StockCalculator.calculate_pe(test_stock)
        assert result == 1000.0, f"Expected P/E of 1000, got {result}"
    
    def test_pe_low_pe_ratio(self, test_stock):
        """Handle low P/E ratios."""
        test_stock.price = 10
        test_stock.eps = 5
        
        result = StockCalculator.calculate_pe(test_stock)
        assert result == 2.0, f"Expected P/E of 2, got {result}"
    
    def test_pe_zero_price_with_parameter(self, test_stock):
        """Handle zero price parameter."""
        test_stock.eps = 5
        
        result = StockCalculator.calculate_pe(test_stock, price=0)
        assert result == 0.0, f"Expected P/E of 0 (0/5), got {result}"

    def test_pe_with_decimal_eps(self, test_stock):
        """P/E should support Decimal EPS values from the DB model."""
        test_stock.price = 100.0
        test_stock.eps = Decimal("4.00")

        result = StockCalculator.calculate_pe(test_stock)
        assert result == 25.0, f"Expected P/E of 25 (100/4), got {result}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

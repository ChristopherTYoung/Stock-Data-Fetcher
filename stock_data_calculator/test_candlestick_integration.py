"""Integration test for candlestick pattern detection in the calculation pipeline."""

import pytest
import pandas as pd
from datetime import datetime
from types import SimpleNamespace
from stock_data_calculator.stock_calculator import StockCalculator, CandlestickPatternDetector


class TestCandlestickIntegration:
    """Test candlestick pattern detection integration with StockCalculator."""
    
    @pytest.fixture
    def hammer_history_data(self):
        """Create history data with a hammer candlestick pattern at the end."""
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        data = {
            'open': [100, 101, 102, 103, 104, 105, 106, 107, 110, 130],
            'high': [101, 102, 103, 104, 105, 106, 107, 108, 115, 140],
            'low': [99, 100, 101, 102, 103, 104, 105, 106, 108, 100],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5, 107.5, 112, 135],
            'volume': [1000000] * 10,
        }
        df = pd.DataFrame(data, index=dates)
        return df
    
    @pytest.fixture
    def inverted_hammer_history_data(self):
        """Create history data with an inverted hammer pattern at the end."""
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        data = {
            'open': [100, 101, 102, 103, 104, 105, 106, 107, 110, 100],
            'high': [101, 102, 103, 104, 105, 106, 107, 108, 115, 140],
            'low': [99, 100, 101, 102, 103, 104, 105, 106, 108, 100],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5, 107.5, 112, 105],
            'volume': [1000000] * 10,
        }
        df = pd.DataFrame(data, index=dates)
        return df
    
    @pytest.fixture
    def hanging_man_history_data(self):
        """Create history data with a hanging man pattern at the end."""
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        data = {
            'open': [100, 101, 102, 103, 104, 105, 106, 107, 110, 135],
            'high': [101, 102, 103, 104, 105, 106, 107, 108, 115, 140],
            'low': [99, 100, 101, 102, 103, 104, 105, 106, 108, 100],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5, 107.5, 112, 130],
            'volume': [1000000] * 10,
        }
        df = pd.DataFrame(data, index=dates)
        return df
    
    @pytest.fixture
    def mock_stock(self):
        """Create a mock stock object."""
        return SimpleNamespace(symbol='TEST', price=100, eps=2.0)
    
    def test_detect_hammer_in_history(self, hammer_history_data, mock_stock):
        """Test detecting hammer pattern from history data."""
        pattern = StockCalculator.calculate_last_candlestick_pattern(
            hammer_history_data, mock_stock
        )
        assert pattern == "hammer"
    
    def test_detect_inverted_hammer_in_history(self, inverted_hammer_history_data, mock_stock):
        """Test detecting inverted hammer pattern from history data."""
        pattern = StockCalculator.calculate_last_candlestick_pattern(
            inverted_hammer_history_data, mock_stock
        )
        assert pattern == "inverted_hammer"
    
    def test_detect_hanging_man_in_history(self, hanging_man_history_data, mock_stock):
        """Test detecting hanging man pattern from history data."""
        pattern = StockCalculator.calculate_last_candlestick_pattern(
            hanging_man_history_data, mock_stock
        )
        assert pattern == "hanging_man"
    
    def test_no_pattern_detection(self, mock_stock):
        """Test with strong bullish candle (no pattern match)."""
        dates = pd.date_range(start='2024-01-01', periods=5, freq='D')
        data = {
            'open': [100, 101, 102, 103, 100],
            'high': [150, 151, 152, 153, 150],
            'low': [100, 101, 102, 103, 100],
            'close': [140, 141, 142, 143, 140],
            'volume': [1000000] * 5,
        }
        df = pd.DataFrame(data, index=dates)
        
        pattern = StockCalculator.calculate_last_candlestick_pattern(df, mock_stock)
        assert pattern is None
    
    def test_empty_dataframe(self, mock_stock):
        """Test with empty dataframe."""
        df = pd.DataFrame()
        
        pattern = StockCalculator.calculate_last_candlestick_pattern(df, mock_stock)
        assert pattern is None
    
    def test_detector_direct_call(self):
        """Test CandlestickPatternDetector directly."""
        # Hammer: close > open, small body at top, long lower wick
        pattern = CandlestickPatternDetector.detect_last_candle(
            open_price=130, close_price=135,
            high=140, low=100
        )
        assert pattern == "hammer"
        
        # Inverted hammer: small body at bottom, long upper wick
        pattern = CandlestickPatternDetector.detect_last_candle(
            open_price=100, close_price=105,
            high=140, low=100
        )
        assert pattern == "inverted_hammer"
        
        # Hanging man: close < open, small body at top, long lower wick
        pattern = CandlestickPatternDetector.detect_last_candle(
            open_price=135, close_price=130,
            high=140, low=100
        )
        assert pattern == "hanging_man"

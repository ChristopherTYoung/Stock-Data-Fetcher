"""
Tests for candlestick pattern detection.

This module tests the calculation of candlestick patterns:
- Hammer: Small body at top, long lower wick
- Inverted Hammer: Small body at bottom, long upper wick  
- Hanging Man: Small body at top, long lower wick
"""

import pytest
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional


@dataclass
class MockStockData:
    """Mock stock history data for testing."""
    symbol: str
    open_price: int
    close_price: int
    high: int
    low: int
    volume: int
    day_and_time: datetime
    is_hourly: bool = True
    
    def get_body_size(self) -> int:
        """Calculate the size of the candlestick body."""
        return abs(self.close_price - self.open_price)
    
    def get_total_size(self) -> int:
        """Calculate the total size (high - low)."""
        return self.high - self.low
    
    def is_bullish(self) -> bool:
        """Check if candle is bullish (close > open)."""
        return self.close_price > self.open_price
    
    def get_lower_wick(self) -> int:
        """Calculate lower wick size."""
        if self.is_bullish():
            return self.open_price - self.low
        else:
            return self.close_price - self.low
    
    def get_upper_wick(self) -> int:
        """Calculate upper wick size."""
        if self.is_bullish():
            return self.high - self.close_price
        else:
            return self.high - self.open_price


class CandlestickPatternDetector:
    """Detects candlestick patterns based on proportions."""
    
    # Pattern thresholds
    HAMMER_BODY_MAX = 0.25  # Body should be max 25% of total
    HAMMER_LOWER_WICK_MIN_RATIO = 2.0  # Lower wick should be at least 2x the body
    HAMMER_UPPER_WICK_MAX = 0.25  # Upper wick should be max 25% of total
    
    INVERTED_HAMMER_BODY_MAX = 0.25  # Body should be max 25% of total
    INVERTED_HAMMER_UPPER_WICK_MIN_RATIO = 2.0  # Upper wick should be at least 2x the body
    INVERTED_HAMMER_LOWER_WICK_MAX = 0.25  # Lower wick should be max 25% of total
    
    HANGING_MAN_BODY_MAX = 0.25  # Body should be max 25% of total
    HANGING_MAN_LOWER_WICK_MIN_RATIO = 2.0  # Lower wick should be at least 2x the body
    HANGING_MAN_UPPER_WICK_MAX = 0.25  # Upper wick should be max 25% of total
    
    @staticmethod
    def detect_hammer(candle: MockStockData) -> bool:
        """
        Detect hammer pattern:
        - Small body at the top (close > open, bullish)
        - Long lower wick (at least 2x the body)
        - Little to no upper wick
        - Bullish reversal pattern
        """
        total_size = candle.get_total_size()
        if total_size == 0:
            return False
        
        # Hammer must be bullish (close > open)
        if not candle.is_bullish():
            return False
        
        body_size = candle.get_body_size()
        lower_wick = candle.get_lower_wick()
        upper_wick = candle.get_upper_wick()
        
        body_ratio = body_size / total_size
        lower_wick_ratio = lower_wick / total_size
        upper_wick_ratio = upper_wick / total_size
        
        # Check if lower wick is at least 2x the body
        wick_to_body_ratio = lower_wick / body_size if body_size > 0 else 0
        
        return (
            body_ratio <= CandlestickPatternDetector.HAMMER_BODY_MAX and
            wick_to_body_ratio >= CandlestickPatternDetector.HAMMER_LOWER_WICK_MIN_RATIO and
            upper_wick_ratio <= CandlestickPatternDetector.HAMMER_UPPER_WICK_MAX
        )
    
    @staticmethod
    def detect_inverted_hammer(candle: MockStockData) -> bool:
        """
        Detect inverted hammer pattern:
        - Small body at the bottom
        - Long upper wick (at least 2x the body)
        - Little to no lower wick
        - Can be bullish or bearish
        """
        total_size = candle.get_total_size()
        if total_size == 0:
            return False
        
        body_size = candle.get_body_size()
        lower_wick = candle.get_lower_wick()
        upper_wick = candle.get_upper_wick()
        
        body_ratio = body_size / total_size
        lower_wick_ratio = lower_wick / total_size
        upper_wick_ratio = upper_wick / total_size
        
        # Check if upper wick is at least 2x the body
        wick_to_body_ratio = upper_wick / body_size if body_size > 0 else 0
        
        return (
            body_ratio <= CandlestickPatternDetector.INVERTED_HAMMER_BODY_MAX and
            wick_to_body_ratio >= CandlestickPatternDetector.INVERTED_HAMMER_UPPER_WICK_MIN_RATIO and
            lower_wick_ratio <= CandlestickPatternDetector.INVERTED_HAMMER_LOWER_WICK_MAX
        )
    
    @staticmethod
    def detect_hanging_man(candle: MockStockData) -> bool:
        """
        Detect hanging man pattern:
        - Small body at the top (close < open, bearish)
        - Long lower wick (at least 2x the body)
        - Little to no upper wick
        - Bearish version of the hammer pattern
        """
        total_size = candle.get_total_size()
        if total_size == 0:
            return False
        
        # Hanging man must be bearish (close < open)
        if candle.is_bullish():
            return False
        
        body_size = candle.get_body_size()
        lower_wick = candle.get_lower_wick()
        upper_wick = candle.get_upper_wick()
        
        body_ratio = body_size / total_size
        lower_wick_ratio = lower_wick / total_size
        upper_wick_ratio = upper_wick / total_size
        
        # Check if lower wick is at least 2x the body
        wick_to_body_ratio = lower_wick / body_size if body_size > 0 else 0
        
        return (
            body_ratio <= CandlestickPatternDetector.HANGING_MAN_BODY_MAX and
            wick_to_body_ratio >= CandlestickPatternDetector.HANGING_MAN_LOWER_WICK_MIN_RATIO and
            upper_wick_ratio <= CandlestickPatternDetector.HANGING_MAN_UPPER_WICK_MAX
        )
    
    @staticmethod
    def get_pattern_name(candle: MockStockData) -> Optional[str]:
        """
        Identify which pattern the candle matches.
        Returns the pattern name or None if no pattern matches.
        """
        # Check in order of specificity
        if CandlestickPatternDetector.detect_hammer(candle):
            return "hammer"
        elif CandlestickPatternDetector.detect_inverted_hammer(candle):
            return "inverted_hammer"
        elif CandlestickPatternDetector.detect_hanging_man(candle):
            return "hanging_man"
        return None
    
    @staticmethod
    def get_pattern_proportions(candle: MockStockData) -> dict:
        """Get the proportions of the candlestick components."""
        total_size = candle.get_total_size()
        if total_size == 0:
            return {
                "body_ratio": 0,
                "lower_wick_ratio": 0,
                "upper_wick_ratio": 0,
                "is_bullish": candle.is_bullish(),
            }
        
        body_size = candle.get_body_size()
        lower_wick = candle.get_lower_wick()
        upper_wick = candle.get_upper_wick()
        
        return {
            "body_ratio": body_size / total_size,
            "lower_wick_ratio": lower_wick / total_size,
            "upper_wick_ratio": upper_wick / total_size,
            "is_bullish": candle.is_bullish(),
            "total_size": total_size,
            "body_size": body_size,
            "lower_wick": lower_wick,
            "upper_wick": upper_wick,
        }


# =====================
# Fixtures
# =====================

@pytest.fixture
def base_datetime():
    """Base datetime for generating test data."""
    return datetime(2024, 1, 1, 10, 0, 0)


@pytest.fixture
def hammer_bullish(base_datetime):
    """Bullish hammer: small body at top, long lower wick."""
    # Total range: 100 to 140 (40 points)
    # Body: 130 to 135 (5 points = 12.5%)
    # Lower wick: 100 to 130 (30 points = 75%, 6x body)
    # Upper wick: 135 to 140 (5 points = 12.5%)
    return MockStockData(
        symbol="AAPL",
        open_price=130,
        close_price=135,
        high=140,
        low=100,
        volume=1000000,
        day_and_time=base_datetime,
    )


@pytest.fixture
def hammer_bearish(base_datetime):
    """Bearish hammer: small body at top, long lower wick."""
    # Similar to bullish but close < open
    return MockStockData(
        symbol="AAPL",
        open_price=135,
        close_price=130,
        high=140,
        low=100,
        volume=1000000,
        day_and_time=base_datetime,
    )


@pytest.fixture
def inverted_hammer_bullish(base_datetime):
    """Bullish inverted hammer: small body at bottom, long upper wick."""
    # Total range: 100 to 140 (40 points)
    # Body: 100 to 105 (5 points = 12.5%)
    # Lower wick: 100 to 100 (0 points = 0%)
    # Upper wick: 105 to 140 (35 points = 87.5%, 7x body)
    return MockStockData(
        symbol="AAPL",
        open_price=100,
        close_price=105,
        high=140,
        low=100,
        volume=1000000,
        day_and_time=base_datetime,
    )


@pytest.fixture
def inverted_hammer_bearish(base_datetime):
    """Bearish inverted hammer: small body at bottom, long upper wick."""
    return MockStockData(
        symbol="AAPL",
        open_price=105,
        close_price=100,
        high=140,
        low=100,
        volume=1000000,
        day_and_time=base_datetime,
    )


@pytest.fixture
def hanging_man(base_datetime):
    """Hanging man: small body at top, long lower wick, bearish (close < open)."""
    # Open: 135, Close: 130 (bearish, body = 5)
    # High: 140, Low: 100 (total = 40)
    # Lower wick: 130 - 100 = 30
    # Upper wick: 140 - 135 = 5
    return MockStockData(
        symbol="AAPL",
        open_price=135,
        close_price=130,
        high=140,
        low=100,
        volume=1000000,
        day_and_time=base_datetime,
    )


@pytest.fixture
def doji(base_datetime):
    """Doji: open == close, similar wicks."""
    return MockStockData(
        symbol="AAPL",
        open_price=120,
        close_price=120,
        high=130,
        low=110,
        volume=1000000,
        day_and_time=base_datetime,
    )


@pytest.fixture
def strong_bullish(base_datetime):
    """Strong bullish candle: large body, no wicks."""
    return MockStockData(
        symbol="AAPL",
        open_price=100,
        close_price=140,
        high=140,
        low=100,
        volume=1000000,
        day_and_time=base_datetime,
    )


@pytest.fixture
def strong_bearish(base_datetime):
    """Strong bearish candle: large body, no wicks."""
    return MockStockData(
        symbol="AAPL",
        open_price=140,
        close_price=100,
        high=140,
        low=100,
        volume=1000000,
        day_and_time=base_datetime,
    )


# =====================
# Tests for MockStockData
# =====================

class TestMockStockData:
    """Test the MockStockData class."""
    
    def test_bullish_candle_properties(self, hammer_bullish):
        """Test properties of a bullish candle."""
        assert hammer_bullish.is_bullish() is True
        assert hammer_bullish.get_body_size() == 5
        assert hammer_bullish.get_total_size() == 40
        assert hammer_bullish.get_lower_wick() == 30
        assert hammer_bullish.get_upper_wick() == 5
    
    def test_bearish_candle_properties(self, hammer_bearish):
        """Test properties of a bearish candle."""
        assert hammer_bearish.is_bullish() is False
        assert hammer_bearish.get_body_size() == 5
        assert hammer_bearish.get_total_size() == 40
        assert hammer_bearish.get_lower_wick() == 30
        assert hammer_bearish.get_upper_wick() == 5
    
    def test_doji_properties(self, doji):
        """Test properties of a doji candle."""
        assert doji.is_bullish() is False  # close == open, not bullish
        assert doji.get_body_size() == 0
        assert doji.get_total_size() == 20


# =====================
# Tests for Hammer Pattern
# =====================

class TestHammerPattern:
    """Test hammer pattern detection."""
    
    def test_hammer_bullish_detection(self, hammer_bullish):
        """Test detection of bullish hammer."""
        assert CandlestickPatternDetector.detect_hammer(hammer_bullish) is True
    
    def test_hammer_bearish_not_detected(self, hammer_bearish):
        """Test that bearish candles are not detected as hammers."""
        # Hammer must be bullish (close > open), so bearish should not match
        assert CandlestickPatternDetector.detect_hammer(hammer_bearish) is False
    
    def test_hammer_proportions(self, hammer_bullish):
        """Test that hammer has correct proportions."""
        props = CandlestickPatternDetector.get_pattern_proportions(hammer_bullish)
        
        assert props["body_ratio"] == pytest.approx(0.125)  # 5/40
        assert props["lower_wick_ratio"] == pytest.approx(0.75)  # 30/40
        assert props["upper_wick_ratio"] == pytest.approx(0.125)  # 5/40
        assert props["is_bullish"] is True
    
    def test_hammer_not_detected_in_strong_bullish(self, strong_bullish):
        """Test that strong bullish candle is not a hammer."""
        assert CandlestickPatternDetector.detect_hammer(strong_bullish) is False
    
    def test_hammer_not_detected_in_strong_bearish(self, strong_bearish):
        """Test that strong bearish candle is not a hammer."""
        assert CandlestickPatternDetector.detect_hammer(strong_bearish) is False


# =====================
# Tests for Inverted Hammer Pattern
# =====================

class TestInvertedHammerPattern:
    """Test inverted hammer pattern detection."""
    
    def test_inverted_hammer_bullish_detection(self, inverted_hammer_bullish):
        """Test detection of bullish inverted hammer."""
        assert CandlestickPatternDetector.detect_inverted_hammer(inverted_hammer_bullish) is True
    
    def test_inverted_hammer_bearish_detection(self, inverted_hammer_bearish):
        """Test detection of bearish inverted hammer."""
        assert CandlestickPatternDetector.detect_inverted_hammer(inverted_hammer_bearish) is True
    
    def test_inverted_hammer_proportions(self, inverted_hammer_bullish):
        """Test that inverted hammer has correct proportions."""
        props = CandlestickPatternDetector.get_pattern_proportions(inverted_hammer_bullish)
        
        assert props["body_ratio"] == pytest.approx(0.125)  # 5/40
        assert props["lower_wick_ratio"] == pytest.approx(0.0)  # 0/40
        assert props["upper_wick_ratio"] == pytest.approx(0.875)  # 35/40
        assert props["is_bullish"] is True
    
    def test_inverted_hammer_not_detected_in_hammer(self, hammer_bullish):
        """Test that hammer is not inverted hammer."""
        assert CandlestickPatternDetector.detect_inverted_hammer(hammer_bullish) is False


# =====================
# Tests for Hanging Man Pattern
# =====================

class TestHangingManPattern:
    """Test hanging man pattern detection."""
    
    def test_hanging_man_detection(self, hanging_man):
        """Test detection of hanging man pattern."""
        assert CandlestickPatternDetector.detect_hanging_man(hanging_man) is True
    
    def test_hanging_man_proportions(self, hanging_man):
        """Test that hanging man has correct proportions."""
        props = CandlestickPatternDetector.get_pattern_proportions(hanging_man)
        
        assert props["body_ratio"] == pytest.approx(0.125)  # 5/40
        assert props["lower_wick_ratio"] == pytest.approx(0.75)  # 30/40
        assert props["upper_wick_ratio"] == pytest.approx(0.125)  # 5/40


# =====================
# Tests for Pattern Detection (Multiple Patterns)
# =====================

class TestPatternDetection:
    """Test overall pattern detection logic."""
    
    def test_hammer_identified(self, hammer_bullish):
        """Test that hammer is correctly identified."""
        pattern = CandlestickPatternDetector.get_pattern_name(hammer_bullish)
        assert pattern == "hammer"
    
    def test_inverted_hammer_identified(self, inverted_hammer_bullish):
        """Test that inverted hammer is correctly identified."""
        pattern = CandlestickPatternDetector.get_pattern_name(inverted_hammer_bullish)
        assert pattern == "inverted_hammer"
    
    def test_hanging_man_identified(self, hanging_man):
        """Test that hanging man is correctly identified."""
        pattern = CandlestickPatternDetector.get_pattern_name(hanging_man)
        assert pattern == "hanging_man"
    
    def test_no_pattern_identified(self, strong_bullish):
        """Test that no pattern is identified for strong candles."""
        pattern = CandlestickPatternDetector.get_pattern_name(strong_bullish)
        assert pattern is None
    
    def test_no_pattern_for_doji(self, doji):
        """Test that no pattern is identified for doji."""
        pattern = CandlestickPatternDetector.get_pattern_name(doji)
        assert pattern is None


# =====================
# Tests for Edge Cases
# =====================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_zero_size_candle(self, base_datetime):
        """Test candle with zero size (high == low)."""
        candle = MockStockData(
            symbol="AAPL",
            open_price=100,
            close_price=100,
            high=100,
            low=100,
            volume=1000000,
            day_and_time=base_datetime,
        )
        
        assert CandlestickPatternDetector.detect_hammer(candle) is False
        assert CandlestickPatternDetector.detect_inverted_hammer(candle) is False
        assert CandlestickPatternDetector.detect_hanging_man(candle) is False
    
    def test_proportions_with_zero_size(self, base_datetime):
        """Test proportions calculation with zero size."""
        candle = MockStockData(
            symbol="AAPL",
            open_price=100,
            close_price=100,
            high=100,
            low=100,
            volume=1000000,
            day_and_time=base_datetime,
        )
        
        props = CandlestickPatternDetector.get_pattern_proportions(candle)
        assert props["body_ratio"] == 0
        assert props["lower_wick_ratio"] == 0
        assert props["upper_wick_ratio"] == 0
    
    def test_very_small_body_relative_to_wicks(self, base_datetime):
        """Test candle with very small body relative to wicks."""
        # Total: 990 points, body: 1 point
        # Open: 500, Close: 501 (bullish, body = 1)
        # High: 1000, Low: 10 (total = 990)
        # Lower wick: 500 - 10 = 490
        # Upper wick: 1000 - 501 = 499
        # This has too large upper wick, so it's not a valid hammer
        candle = MockStockData(
            symbol="AAPL",
            open_price=500,
            close_price=501,
            high=1000,
            low=10,
            volume=1000000,
            day_and_time=base_datetime,
        )
        
        assert CandlestickPatternDetector.detect_hammer(candle) is False
    
    def test_hammer_at_threshold_lower_wick(self, base_datetime):
        """Test hammer pattern at the exact lower wick threshold."""
        # Create a hammer at the exact threshold: body at 25%, wick ratio at 2.0
        # Body: 1, Lower wick: 2, Upper wick: 0
        # Open: 2, Close: 3, High: 3, Low: 0
        candle = MockStockData(
            symbol="AAPL",
            open_price=2,
            close_price=3,
            high=3,
            low=0,
            volume=1000000,
            day_and_time=base_datetime,
        )
        
        props = CandlestickPatternDetector.get_pattern_proportions(candle)
        # Body ratio is 1/3 = 33.3%, which exceeds 25% max, so fails
        assert props["body_ratio"] == pytest.approx(1/3)
        assert CandlestickPatternDetector.detect_hammer(candle) is False

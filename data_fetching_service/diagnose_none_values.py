#!/usr/bin/env python
"""Diagnostic script to understand why calculations return None after gap filling."""
import logging
from datetime import datetime, timedelta
import pandas as pd
from stock_calculator import StockCalculator

# Configure logging to see detailed output
logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s - %(levelname)s - %(message)s'
)

def test_none_dataframe_scenario():
    """Test when history_data is None - simulates potential gap filling issue."""
    print("\n" + "="*80)
    print("TEST 1: Calculations with None history_data (no Polygon data)")
    print("="*80)
    
    result = StockCalculator.calculate_price(None, None)
    print(f"Result: {result}")
    print(f"Expected: None (correct behavior when no data available)")
    

def test_empty_dataframe_scenario():
    """Test when history_data is empty DataFrame - another potential issue."""
    print("\n" + "="*80)
    print("TEST 2: Calculations with empty DataFrame")
    print("="*80)
    
    empty_df = pd.DataFrame()
    
    result = StockCalculator.calculate_price(empty_df, None)
    print(f"Result: {result}")
    print(f"Expected: None (correct behavior for empty data)")
    

def test_valid_dataframe_scenario():
    """Test with valid data - should work."""
    print("\n" + "="*80)
    print("TEST 3: Calculations with valid DataFrame (SHOULD WORK)")
    print("="*80)
    
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
    
    price = StockCalculator.calculate_price(df, None)
    print(f"Result: {price}")
    print(f"Expected: 111 (last close price)")
    

def test_dataframe_without_index():
    """Test DataFrame without proper index - might cause None."""
    print("\n" + "="*80)
    print("TEST 4: DataFrame without proper timestamp index")
    print("="*80)
    
    # DataFrame with 'timestamp' column instead of index
    now = datetime.utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    df = pd.DataFrame({
        'timestamp': [now - timedelta(days=1), now],
        'open': [100, 105],
        'high': [102, 107],
        'low': [99, 104],
        'close': [101, 106],
        'volume': [1000, 1100]
    })
    
    print(f"DataFrame structure:\n{df}")
    
    price = StockCalculator.calculate_price(df, None)
    print(f"Result: {price}")
    print(f"Expected: 106 (last close price)")


if __name__ == "__main__":
    test_none_dataframe_scenario()
    test_empty_dataframe_scenario()
    test_valid_dataframe_scenario()
    test_dataframe_without_index()
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print("If TEST 3 works but TEST 4 returns None, the issue is likely that:")
    print("- history_data is being passed with 'timestamp' as a column, not an index")
    print("- OR history_data is None or empty when gap filling occurs")
    print("\nCheck the Polygon API response format in get_historical_data()")
    print("="*80 + "\n")

"""Test script for Polygon API integration."""
from datetime import datetime, timedelta
from data_fetcher import DataFetcher
import os

def test_get_historical_data():
    """Test the get_historical_data method with Polygon API."""

    api_key = os.environ.get('POLYGON_API_KEY')
    if not api_key:
        print("❌ ERROR: POLYGON_API_KEY environment variable not set")
        print("Please set it with: export POLYGON_API_KEY='your_api_key'")
        return False
    
    print(f"✓ API Key found: {api_key[:8]}...")

    fetcher = DataFetcher()

    print("\n" + "="*60)
    print("Test 1: Fetching daily data for AAPL")
    print("="*60)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    try:
        daily_df = fetcher.get_historical_data(
            ticker='AAPL',
            from_date=start_date.strftime('%Y-%m-%d'),
            to_date=end_date.strftime('%Y-%m-%d'),
            timespan='day',
            multiplier=1
        )
        
        if daily_df.empty:
            print("❌ No daily data returned")
            return False
        
        print(f"✓ Successfully fetched {len(daily_df)} days of data")
        print(f"\nFirst few rows:")
        print(daily_df.head())
        print(f"\nColumns: {daily_df.columns.tolist()}")
        
    except Exception as e:
        print(f"❌ Error fetching daily data: {e}")
        return False

    print("\n" + "="*60)
    print("Test 2: Fetching hourly data for AAPL")
    print("="*60)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)
    
    try:
        hourly_df = fetcher.get_historical_data(
            ticker='AAPL',
            from_date=start_date.strftime('%Y-%m-%d'),
            to_date=end_date.strftime('%Y-%m-%d'),
            timespan='hour',
            multiplier=1
        )
        
        if hourly_df.empty:
            print("❌ No hourly data returned")
            return False
        
        print(f"✓ Successfully fetched {len(hourly_df)} hours of data")
        print(f"\nFirst few rows:")
        print(hourly_df.head())
        
    except Exception as e:
        print(f"❌ Error fetching hourly data: {e}")
        return False

    print("\n" + "="*60)
    print("Test 3: Fetching minute data for AAPL")
    print("="*60)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    try:
        minute_df = fetcher.get_historical_data(
            ticker='AAPL',
            from_date=start_date.strftime('%Y-%m-%d'),
            to_date=end_date.strftime('%Y-%m-%d'),
            timespan='minute',
            multiplier=1
        )
        
        if minute_df.empty:
            print("⚠️  No minute data returned (this may be normal if API limits are hit)")
        else:
            print(f"✓ Successfully fetched {len(minute_df)} minutes of data")
            print(f"\nFirst few rows:")
            print(minute_df.head())
        
    except Exception as e:
        print(f"⚠️  Error fetching minute data: {e}")
        print("(Note: Minute data may require a paid Polygon subscription)")
    
    print("\n" + "="*60)
    print("✓ Basic Polygon API integration tests passed!")
    print("="*60)
    return True


def test_fetch_all_tickers():
    """Test fetch_all_tickers_historical_data with Polygon API."""
    print("\n" + "="*60)
    print("Test 4: Testing fetch_all_tickers_historical_data")
    print("="*60)
    
    fetcher = DataFetcher()
    end_date = datetime.now()

    print("Fetching data for ['AAPL']...")
    
    try:
        result = fetcher.fetch_all_tickers_historical_data(
            tickers=['AAPL'],
            end_date=end_date
        )
        
        print(f"\nResults:")
        print(f"Total tickers: {result['summary']['total_tickers']}")
        print(f"Successful: {result['summary']['successful']}")
        print(f"Failed: {result['summary']['failed']}")
        print(f"Total rows inserted: {result['summary']['total_rows_inserted']}")
        
        if result['summary']['successful'] > 0:
            print("\n✓ fetch_all_tickers_historical_data works with Polygon API!")
            for ticker, details in result['results'].items():
                if details.get('success'):
                    print(f"  {ticker}: {details.get('rows_inserted', 0)} rows inserted")
        else:
            print("\n❌ No tickers were successfully fetched")
            if result['summary']['failed_tickers']:
                print(f"Failed tickers: {result['summary']['failed_tickers']}")
        
        return result['summary']['successful'] > 0
        
    except Exception as e:
        print(f"❌ Error in fetch_all_tickers_historical_data: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    print("\n" + "#"*60)
    print("# Polygon API Integration Test")
    print("#"*60)

    basic_success = test_get_historical_data()
    
    if basic_success:
        full_success = test_fetch_all_tickers()
    else:
        print("\n⚠️  Skipping full integration test due to basic test failures")
        full_success = False
    
    print("\n" + "#"*60)
    if basic_success and full_success:
        print("# ✓ All tests passed!")
    else:
        print("# ⚠️  Some tests failed - see details above")
    print("#"*60 + "\n")

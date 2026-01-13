"""Test script to verify Polygon ticker fetching and pagination."""
import os
from polygon import RESTClient


def test_ticker_pagination():
    """Test that we can fetch all US tickers with proper pagination."""
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("❌ ERROR: POLYGON_API_KEY environment variable not set")
        return False
    
    print(f"✓ API Key found: {api_key[:8]}...")
    
    client = RESTClient(api_key=api_key)
    
    print("\n" + "="*60)
    print("Fetching US Common Stocks from Polygon")
    print("="*60)
    
    tickers = []
    next_url = None
    page_count = 0
    
    try:
        while True:
            if next_url:
                print(f"\nFetching page {page_count + 1} using next_url...")
                response = client.list_tickers(next_url=next_url)
            else:
                print(f"\nFetching page {page_count + 1} (initial request)...")
                response = client.list_tickers(
                    market='stocks',
                    active=True,
                    limit=1000
                )
            
            page_tickers = []
            us_common_stocks = []
            
            for ticker in response:
                page_tickers.append(ticker.ticker)

                if (hasattr(ticker, 'type') and ticker.type == 'CS' and
                    hasattr(ticker, 'locale') and ticker.locale == 'us'):
                    symbol = ticker.ticker
                    
                    if (symbol and
                        len(symbol) <= 10 and
                        not symbol.startswith('$') and
                        not symbol.startswith('^') and
                        '.' not in symbol):
                        us_common_stocks.append(symbol)
                        tickers.append(symbol)
            
            page_count += 1
            print(f"  Page {page_count}:")
            print(f"    - Raw tickers in page: {len(page_tickers)}")
            print(f"    - US common stocks in page: {len(us_common_stocks)}")
            print(f"    - Total US common stocks so far: {len(tickers)}")

            if us_common_stocks:
                print(f"    - Sample from this page: {us_common_stocks[:5]}")

            if hasattr(response, 'next_url') and response.next_url:
                next_url = response.next_url
                print(f"    - More pages available: Yes")
            else:
                print(f"    - More pages available: No (last page)")
                break
        
        print("\n" + "="*60)
        print("RESULTS")
        print("="*60)
        print(f"Total pages fetched: {page_count}")
        print(f"Total US common stocks: {len(tickers)}")
        print(f"\nFirst 20 tickers: {tickers[:20]}")
        print(f"\nLast 20 tickers: {tickers[-20:]}")

        if len(tickers) < 1000:
            print(f"\n⚠️  WARNING: Only got {len(tickers)} tickers, expected 5000+")
            print("This suggests pagination might not be working correctly")
            return False
        elif len(tickers) < 3000:
            print(f"\n⚠️  Got {len(tickers)} tickers - lower than expected (5000-7000)")
            print("Pagination is working but filters might be too aggressive")
            return True
        else:
            print(f"\n✓ SUCCESS: Got {len(tickers)} tickers - looks good!")
            return True
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_with_orchestrator_function():
    """Test using the actual orchestrator function."""
    print("\n" + "#"*60)
    print("# Testing with orchestrator function")
    print("#"*60)

    try:
        from orchestrator import init_polygon_client, fetch_stock_list_from_polygon
        
        print("\nInitializing Polygon client...")
        init_polygon_client()
        
        print("Fetching stock list...")
        tickers = fetch_stock_list_from_polygon()
        
        print(f"\n✓ Got {len(tickers)} tickers from orchestrator function")
        print(f"First 10: {tickers[:10]}")
        print(f"Last 10: {tickers[-10:]}")
        
        if len(tickers) >= 1000:
            print(f"\n✓ SUCCESS: Orchestrator function is working!")
            return True
        else:
            print(f"\n⚠️  WARNING: Only got {len(tickers)} tickers")
            return False
            
    except Exception as e:
        print(f"\n❌ ERROR testing orchestrator function: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    print("\n" + "#"*60)
    print("# Polygon Ticker Fetch Test")
    print("#"*60)

    print("\nTest 1: Direct API Pagination Test")
    print("-" * 60)
    test1_passed = test_ticker_pagination()
    
    print("\n" + "-"*60)
    print("\nTest 2: Orchestrator Function Test")
    print("-" * 60)
    test2_passed = test_with_orchestrator_function()

    print("\n" + "#"*60)
    print("# TEST SUMMARY")
    print("#"*60)
    print(f"Direct API Test: {'✓ PASSED' if test1_passed else '✗ FAILED'}")
    print(f"Orchestrator Test: {'✓ PASSED' if test2_passed else '✗ FAILED'}")
    print("#"*60 + "\n")

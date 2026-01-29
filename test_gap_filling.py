#!/usr/bin/env python3
"""
Test script for gap detection and filling functionality.

This script tests the gap detection and filling on real data in your Kubernetes cluster.
"""

import requests
import time
import argparse
from typing import Dict, Any


class GapFillingTester:
    """Test gap detection and filling functionality."""
    
    def __init__(self, base_url: str):
        """
        Initialize the tester.
        
        Args:
            base_url: Base URL of the data fetcher service (e.g., http://localhost:8000)
        """
        self.base_url = base_url.rstrip('/')
    
    def check_health(self) -> bool:
        """Check if the service is healthy."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            response.raise_for_status()
            print("✓ Service is healthy")
            return True
        except Exception as e:
            print(f"✗ Service health check failed: {e}")
            return False
    
    def fetch_initial_data(self, tickers: list[str]) -> Dict[str, Any]:
        """Fetch initial data for tickers to create a baseline."""
        print(f"\n📥 Fetching initial data for {tickers}...")
        try:
            response = requests.post(
                f"{self.base_url}/fetch-stock-data",
                json={"tickers": tickers},
                timeout=300
            )
            response.raise_for_status()
            result = response.json()
            
            print(f"✓ Initial fetch completed:")
            print(f"  - Successful: {result['summary']['successful']}")
            print(f"  - Failed: {result['summary']['failed']}")
            print(f"  - Total rows: {result['summary']['total_rows_inserted']}")
            
            return result
        except Exception as e:
            print(f"✗ Initial fetch failed: {e}")
            return {}
    
    def check_gaps(self, ticker: str) -> Dict[str, Any]:
        """Check for gaps in a ticker's data."""
        print(f"\n🔍 Checking for gaps in {ticker}...")
        try:
            response = requests.get(f"{self.base_url}/check-gaps/{ticker}", timeout=30)
            response.raise_for_status()
            result = response.json()
            
            print(f"✓ Gap check completed for {ticker}:")
            print(f"  - Gaps found: {result['gaps_found']}")
            
            if result['gaps_found'] > 0:
                print(f"  - Gap details:")
                for gap in result['gaps']:
                    gap_type = "hourly" if gap['is_hourly'] else "minute"
                    print(f"    • {gap_type}: {gap['start']} to {gap['end']} ({gap['duration_days']} days)")
            
            return result
        except Exception as e:
            print(f"✗ Gap check failed: {e}")
            return {}
    
    def fill_gaps(self, ticker: str) -> Dict[str, Any]:
        """Detect and fill gaps for a ticker."""
        print(f"\n🔧 Filling gaps for {ticker}...")
        try:
            response = requests.post(f"{self.base_url}/fill-gaps/{ticker}", timeout=600)
            response.raise_for_status()
            result = response.json()
            
            print(f"✓ Gap filling completed for {ticker}:")
            print(f"  - Gaps found: {result['gaps_found']}")
            print(f"  - Gaps filled: {result['gaps_filled']}")
            print(f"  - Gaps failed: {result['gaps_failed']}")
            print(f"  - Total rows inserted: {result['total_rows_inserted']}")
            
            if result.get('filled_gaps'):
                print(f"  - Successfully filled gaps:")
                for gap in result['filled_gaps']:
                    gap_type = "hourly" if gap['is_hourly'] else "minute"
                    print(f"    • {gap_type}: {gap['start']} to {gap['end']} ({gap['rows_inserted']} rows)")
            
            if result.get('failed_gaps'):
                print(f"  - Failed gaps:")
                for gap in result['failed_gaps']:
                    gap_type = "hourly" if gap['is_hourly'] else "minute"
                    print(f"    • {gap_type}: {gap['start']} to {gap['end']} - {gap['error']}")
            
            return result
        except Exception as e:
            print(f"✗ Gap filling failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"   Response: {e.response.text}")
            return {}
    
    def run_test(self, ticker: str, fetch_initial: bool = False):
        """Run a complete test cycle for a ticker."""
        print(f"\n{'='*60}")
        print(f"Testing Gap Detection & Filling for {ticker}")
        print(f"{'='*60}")
        
        # Check health
        if not self.check_health():
            return
        
        # Optionally fetch initial data
        if fetch_initial:
            self.fetch_initial_data([ticker])
            time.sleep(2)
        
        # Check for gaps
        gap_result = self.check_gaps(ticker)
        
        if not gap_result or gap_result.get('gaps_found', 0) == 0:
            print(f"\n✓ No gaps found for {ticker} - test complete!")
            return
        
        # Wait a moment
        time.sleep(2)
        
        # Fill gaps
        fill_result = self.fill_gaps(ticker)
        
        # Verify gaps are filled
        time.sleep(2)
        print(f"\n🔍 Re-checking for gaps after filling...")
        final_gap_result = self.check_gaps(ticker)
        
        print(f"\n{'='*60}")
        print(f"Test Summary for {ticker}")
        print(f"{'='*60}")
        print(f"Initial gaps: {gap_result.get('gaps_found', 0)}")
        print(f"Gaps filled: {fill_result.get('gaps_filled', 0)}")
        print(f"Remaining gaps: {final_gap_result.get('gaps_found', 0)}")
        print(f"Total rows added: {fill_result.get('total_rows_inserted', 0)}")
        
        if final_gap_result.get('gaps_found', 0) == 0:
            print(f"\n✓✓✓ SUCCESS! All gaps filled for {ticker}")
        elif final_gap_result.get('gaps_found', 0) < gap_result.get('gaps_found', 0):
            print(f"\n✓ PARTIAL SUCCESS - Some gaps filled for {ticker}")
        else:
            print(f"\n✗ FAILED - Gaps remain for {ticker}")


def main():
    parser = argparse.ArgumentParser(description='Test gap detection and filling')
    parser.add_argument('--url', default='http://localhost:8000',
                      help='Base URL of the data fetcher service')
    parser.add_argument('--ticker', default='AAPL',
                      help='Stock ticker to test')
    parser.add_argument('--fetch-initial', action='store_true',
                      help='Fetch initial data before testing gaps')
    parser.add_argument('--port-forward', action='store_true',
                      help='Show kubectl port-forward command')
    
    args = parser.parse_args()
    
    if args.port_forward:
        print("\nTo access the service, run:")
        print("  kubectl port-forward -n stock-data-fetcher svc/data-fetcher-service 8000:8000")
        print("\nThen run this script with: --url http://localhost:8000\n")
        return
    
    tester = GapFillingTester(args.url)
    tester.run_test(args.ticker, fetch_initial=args.fetch_initial)


if __name__ == "__main__":
    main()

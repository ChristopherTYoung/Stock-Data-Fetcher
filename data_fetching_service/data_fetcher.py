"""Data fetching service for stock data using yfinance."""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import pandas as pd
import logging
import time
import yfinance as yf
from database import get_db, StockHistory, Stock, Blacklist
from gap_detector import GapDetector

logger = logging.getLogger(__name__)


class DataFetcher:
    """Handles fetching and storing stock data from yfinance."""
    
    def __init__(self, max_gap_fill_retries: int = 3):
        self.rate_limited = False
        self.rate_limit_reset_time = None
        self.gap_detector = GapDetector()
        self.max_gap_fill_retries = max_gap_fill_retries
    
    def add_to_blacklist(self, ticker: str, gap_start: datetime) -> None:
        """Add a gap to the blacklist."""
        with get_db() as db:
            blacklist_entry = Blacklist(
                stock_symbol=ticker,
                timestamp=gap_start,
                time_added=datetime.now()
            )
            db.add(blacklist_entry)
            db.commit()
            logger.info(f"Added {ticker} gap at {gap_start} to blacklist")
    
    def get_blacklist(self, ticker: str = None) -> List[Dict[str, Any]]:
        """Get blacklist entries, optionally filtered by ticker."""
        with get_db() as db:
            from sqlalchemy import select
            
            if ticker:
                query = select(Blacklist).where(Blacklist.stock_symbol == ticker.upper())
            else:
                query = select(Blacklist)
            
            results = db.execute(query).fetchall()
            
            return [
                {
                    "id": row[0].id,
                    "stock_symbol": row[0].stock_symbol,
                    "timestamp": row[0].timestamp.isoformat(),
                    "time_added": row[0].time_added.isoformat()
                }
                for row in results
            ]
    
    def clear_blacklist(self, ticker: str = None) -> int:
        """Clear blacklist entries, optionally for a specific ticker."""
        with get_db() as db:
            if ticker:
                from sqlalchemy import delete
                result = db.execute(
                    delete(Blacklist).where(Blacklist.stock_symbol == ticker.upper())
                )
                count = result.rowcount
            else:
                from sqlalchemy import delete
                result = db.execute(delete(Blacklist))
                count = result.rowcount
            
            db.commit()
            logger.info(f"Cleared {count} blacklist entries{f' for {ticker}' if ticker else ''}")
            return count
    
    def ensure_stock_exists(self, ticker: str, db) -> None:
        """Ensure the stock exists in the stock table. If not, fetch company name and insert."""
        from sqlalchemy import select
        
        # Check if stock already exists
        result = db.execute(select(Stock).where(Stock.symbol == ticker)).first()
        
        if result is None:
            # Stock doesn't exist, fetch company info from yfinance
            try:
                stock_info = yf.Ticker(ticker)
                company_name = stock_info.info.get('longName', stock_info.info.get('shortName', ticker))
                
                # Create new stock record
                new_stock = Stock(
                    symbol=ticker,
                    company_name=company_name,
                    updated_at=datetime.now()
                )
                db.add(new_stock)
                db.commit()
                logger.info(f"Added stock {ticker} ({company_name}) to stock table")
            except Exception as e:
                logger.warning(f"Could not fetch company name for {ticker}, using ticker as name: {e}")
                # Fallback: use ticker as company name
                new_stock = Stock(
                    symbol=ticker,
                    company_name=ticker,
                    updated_at=datetime.now()
                )
                db.add(new_stock)
                db.commit()
                logger.info(f"Added stock {ticker} to stock table with ticker as name")

    def save_stock_data_to_db(self, ticker: str, df: pd.DataFrame, is_hourly: bool = False) -> int:
        """Save stock data DataFrame to database."""
        if df.empty:
            logger.warning(f"No data to save for {ticker} (is_hourly={is_hourly})")
            return 0
        
        rows_inserted = 0
        
        with get_db() as db:
            self.ensure_stock_exists(ticker, db)
            
            for timestamp, row in df.iterrows():
                try:
                    # Handle both uppercase (yfinance) and lowercase column names
                    open_col = 'Open' if 'Open' in row else 'open'
                    close_col = 'Close' if 'Close' in row else 'close'
                    high_col = 'High' if 'High' in row else 'high'
                    low_col = 'Low' if 'Low' in row else 'low'
                    volume_col = 'Volume' if 'Volume' in row else 'volume'
                    
                    stock_record = StockHistory(
                        stock_symbol=ticker,
                        day_and_time=timestamp,
                        open_price=int(row[open_col] * 100),  # Convert to cents
                        close_price=int(row[close_col] * 100),
                        high=int(row[high_col] * 100),
                        low=int(row[low_col] * 100),
                        volume=int(row[volume_col]),
                        is_hourly=is_hourly
                    )
                    db.add(stock_record)
                    rows_inserted += 1
                except Exception as e:
                    logger.error(f"Error inserting row for {ticker} at {timestamp}: {str(e)}")
                    continue
            
            # Commit in batches for better performance
            if rows_inserted > 0:
                db.commit()
                logger.info(f"✓ SAVED {rows_inserted} rows for {ticker} (is_hourly={is_hourly})")
            else:
                logger.error(f"✗ FAILED to save any data for {ticker} (is_hourly={is_hourly})")
                raise Exception(f"Failed to insert any rows for {ticker}")
        
        return rows_inserted

    def fetch_ticker_hourly_data(self, ticker: str, max_retries: int = 3) -> pd.DataFrame:
        """Fetch 2 years of hourly data for a ticker."""
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching hourly data for {ticker} (attempt {attempt + 1}/{max_retries})")
                df = yf.download(
                    tickers=[ticker],
                    period="2y",
                    interval='1h'
                )
                
                if not df.empty:
                    logger.info(f"Fetched {len(df)} hourly rows for {ticker}")
                    return df
                
                if attempt < max_retries - 1:
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"Error fetching hourly data for {ticker}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        time.sleep(3)
        
        return pd.DataFrame()

    def fetch_ticker_minute_data(self, ticker: str, start_date: datetime, end_date: datetime, max_retries: int = 3) -> pd.DataFrame:
        """Fetch minute-level data for a ticker within a date range."""
        all_data = []
        current_start = start_date

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=6), end_date)
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Fetching minute data for {ticker} from {current_start.date()} to {current_end.date()}")
                    df = yf.download(
                        tickers=ticker,
                        start=current_start.strftime('%Y-%m-%d'),
                        end=current_end.strftime('%Y-%m-%d'),
                        interval='1m'
                    )
                    
                    if not df.empty:
                        all_data.append(df)
                        logger.info(f"Fetched {len(df)} minute rows for {ticker}")
                    break
                    
                except Exception as e:
                    logger.error(f"Error fetching minute data for {ticker}: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(5)
            
            current_start = current_end
            time.sleep(3)  # Delay between chunks
        
        if all_data:
            combined_df = pd.concat(all_data)
            logger.info(f"Total minute data for {ticker}: {len(combined_df)} rows")
            return combined_df
        
        return pd.DataFrame()

    def fetch_all_tickers_historical_data(self, tickers: List[str], end_date: datetime) -> Dict[str, Any]:
        """Fetch 2 years of hourly data + 1 month of minute data for tickers."""
        if self.rate_limited:
            return {
                "results": {},
                "summary": {
                    "total_tickers": len(tickers),
                    "successful": 0,
                    "failed": len(tickers),
                    "failed_tickers": tickers,
                    "total_rows_inserted": 0,
                    "error": "Service is rate limited"
                }
            }
        
        minute_start_date = end_date - timedelta(days=30)
        
        logger.info(f"Fetching data for {len(tickers)} tickers:")
        logger.info(f"  - Minute: {minute_start_date.date()} to {end_date.date()} (1 month)")
        
        results = {}
        failed_tickers = []
        total_rows_inserted = 0
        
        for idx, ticker in enumerate(tickers, 1):
            try:
                logger.info(f"Processing {ticker} ({idx}/{len(tickers)})")
                ticker_rows = 0
                
                # Fetch hourly data (2 years)
                logger.info(f"  Fetching 2 years of hourly data for {ticker}...")
                hourly_df = self.fetch_ticker_hourly_data(ticker)
                if not hourly_df.empty:
                    rows = self.save_stock_data_to_db(ticker, hourly_df, is_hourly=True)
                    ticker_rows += rows
                    logger.info(f"  Saved {rows} hourly rows for {ticker}")
                
                # Fetch minute data (1 month)
                logger.info(f"  Fetching 1 month of minute data for {ticker}...")
                minute_df = self.fetch_ticker_minute_data(ticker, minute_start_date, end_date)
                if not minute_df.empty:
                    rows = self.save_stock_data_to_db(ticker, minute_df, is_hourly=False)
                    ticker_rows += rows
                    logger.info(f"  Saved {rows} minute rows for {ticker}")
                
                total_rows_inserted += ticker_rows
                
                if ticker_rows > 0:
                    results[ticker] = {
                        "success": True,
                        "rows_inserted": ticker_rows,
                        "hourly_rows": len(hourly_df) if not hourly_df.empty else 0,
                        "minute_rows": len(minute_df) if not minute_df.empty else 0
                    }
                    logger.info(f"✓✓✓ SUCCESS: {ticker} - {ticker_rows} total rows saved to database")
                else:
                    results[ticker] = {
                        "success": False,
                        "error": "No data returned from yfinance"
                    }
                    failed_tickers.append(ticker)
                    logger.error(f"✗✗✗ FAILED: {ticker} - No data fetched from yfinance")
                
                # Delay between tickers to avoid rate limiting
                if idx < len(tickers):
                    time.sleep(3)
                    
            except Exception as e:
                results[ticker] = {
                    "success": False,
                    "error": str(e)
                }
                failed_tickers.append(ticker)
                logger.error(f"Failed to process data for {ticker}: {str(e)}")
        
        return {
            "results": results,
            "summary": {
                "total_tickers": len(tickers),
                "successful": len(tickers) - len(failed_tickers),
                "failed": len(failed_tickers),
                "failed_tickers": failed_tickers,
                "total_rows_inserted": total_rows_inserted
            }
        }
    
    def detect_and_fill_gaps(self, ticker: str, max_retries: int = None) -> Dict[str, Any]:
        """        Detect gaps in a stock's historical data and attempt to fill them.
        Failed gaps after max retries will be added to the blacklist.
        
        Args:
            ticker: Stock symbol to check and fill gaps for
            max_retries: Maximum number of retry attempts per gap (defaults to instance value)
            
        Returns:
            Dictionary containing gap detection results and filling status
        """
        ticker = ticker.upper()
        max_retries = max_retries or self.max_gap_fill_retries
        logger.info(f"Detecting gaps for {ticker}...")
        
        # Detect gaps
        gaps = self.gap_detector.check_for_gaps(ticker)
        
        if not gaps:
            return {
                "ticker": ticker,
                "gaps_found": 0,
                "gaps_filled": 0,
                "message": "No gaps detected"
            }
        
        logger.info(f"Found {len(gaps)} gaps for {ticker}, attempting to fill them (max {max_retries} retries per gap)...")
        
        filled_gaps = []
        failed_gaps = []
        blacklisted_gaps = []
        total_rows_inserted = 0
        
        for gap_start, gap_end, is_hourly in gaps:
            retry_count = 0
            gap_filled = False
            
            while retry_count < max_retries and not gap_filled:
                try:
                    retry_msg = f" (retry {retry_count + 1}/{max_retries})" if retry_count > 0 else ""
                    logger.info(f"Filling gap for {ticker}: {gap_start} to {gap_end} (hourly={is_hourly}){retry_msg}")
                    
                    if is_hourly:
                        # For hourly gaps, fetch hourly data
                        df = self.fetch_ticker_hourly_data(ticker)
                        if not df.empty:
                            # Filter to only the gap period
                            df = df[(df.index >= gap_start) & (df.index <= gap_end)]
                    else:
                        # For minute gaps, fetch minute data for the specific range
                        df = self.fetch_ticker_minute_data(ticker, gap_start, gap_end)
                    
                    if not df.empty:
                        rows_inserted = self.save_stock_data_to_db(ticker, df, is_hourly=is_hourly)
                        total_rows_inserted += rows_inserted
                        filled_gaps.append({
                            "start": gap_start.isoformat(),
                            "end": gap_end.isoformat(),
                            "is_hourly": is_hourly,
                            "rows_inserted": rows_inserted,
                            "retries": retry_count
                        })
                        logger.info(f"✓ Filled gap with {rows_inserted} rows after {retry_count} retries")
                        gap_filled = True
                    else:
                        retry_count += 1
                        if retry_count < max_retries:
                            logger.warning(f"✗ No data available, retrying... ({retry_count}/{max_retries})")
                            time.sleep(3)  # Wait before retry
                        else:
                            logger.warning(f"✗ Could not fill gap after {max_retries} retries - adding to blacklist")
                    
                except Exception as e:
                    retry_count += 1
                    logger.error(f"Error filling gap for {ticker} ({gap_start} to {gap_end}): {str(e)}")
                    if retry_count < max_retries:
                        logger.info(f"Retrying... ({retry_count}/{max_retries})")
                        time.sleep(3)
            
            # If gap wasn't filled after all retries, add to blacklist
            if not gap_filled:
                try:
                    self.add_to_blacklist(ticker, gap_start)
                    blacklisted_gaps.append({
                        "start": gap_start.isoformat(),
                        "end": gap_end.isoformat(),
                        "is_hourly": is_hourly,
                        "error": f"Failed after {max_retries} retries - added to blacklist"
                    })
                except Exception as e:
                    logger.error(f"Error adding gap to blacklist: {e}")
                    failed_gaps.append({
                        "start": gap_start.isoformat(),
                        "end": gap_end.isoformat(),
                        "is_hourly": is_hourly,
                        "error": f"Failed after {max_retries} retries (blacklist failed: {str(e)})"
                    })
            
            # Delay between gap fills to avoid rate limiting
            if not gap_filled:
                time.sleep(2)
        
        return {
            "ticker": ticker,
            "gaps_found": len(gaps),
            "gaps_filled": len(filled_gaps),
            "gaps_failed": len(failed_gaps),
            "gaps_blacklisted": len(blacklisted_gaps),
            "total_rows_inserted": total_rows_inserted,
            "filled_gaps": filled_gaps,
            "failed_gaps": failed_gaps,
            "blacklisted_gaps": blacklisted_gaps
        }

"""Data fetching service for stock data using Polygon."""
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple
import pandas as pd
import logging
import time
from polygon import RESTClient
from database import get_db, Stock, StockHistory
from gap_detector import GapDetector
from database_service import DatabaseService
from sqlalchemy import select
import os

logger = logging.getLogger(__name__)
API_KEY = os.environ.get('POLYGON_API_KEY')


class DataFetcher:
    """Handles fetching and storing stock data from Polygon."""
    
    def __init__(self, max_gap_fill_retries: int = 3):
        self.rate_limited = False
        self.rate_limit_reset_time = None
        self.gap_detector = GapDetector()
        self.db_service = DatabaseService()
        self.max_gap_fill_retries = max_gap_fill_retries
        self.calculated_fields = ["price", "high52", "low52", "percent_change"]
    def update_stock_calcuated_fields(self, stock: Stock, db_data, history_data) -> None:
        """Update calculated fields for a stock in the database."""
        update_dict = {}
        for field in self.calculated_fields:
            ## check if the field exists in the stock object and is not None
            if stock and hasattr(stock, field) and getattr(stock, field) is not None:
                pass
            else:
                update_dict[field] = self.calculate(field, stock, db_data, history_data, update_dict)
            self.db_service.update_stock(stock.symbol, update_dict)
                
    def calculate(self, field: str, stock: Stock, db_data, history_data, update_dict) -> Any:
        """Calculate the value for a specific field based on stock_history data from the db"""
        if field == "price":
            with get_db() as db:
                latest_record = db.query(StockHistory).filter(
                    StockHistory.stock_symbol == stock.symbol,
                    StockHistory.is_hourly == False
                ).order_by(StockHistory.day_and_time.desc()).first()
                if latest_record:
                    return latest_record.close_price
                else:
                    return None 
        if field == "high52":
            with get_db() as db:
                one_year_ago = datetime.utcnow() - timedelta(days=365)
                high_record = db.query(StockHistory).filter(
                    StockHistory.stock_symbol == stock.symbol,
                    StockHistory.is_hourly == False,
                    StockHistory.day_and_time >= one_year_ago
                ).order_by(StockHistory.high.desc()).first()
                if high_record:
                    return high_record.high
                else:
                    return None
        if field == "low52":
            with get_db() as db:
                one_year_ago = datetime.utcnow() - timedelta(days=365)
                low_record = db.query(StockHistory).filter(
                    StockHistory.stock_symbol == stock.symbol,
                    StockHistory.is_hourly == False,
                    StockHistory.day_and_time >= one_year_ago
                ).order_by(StockHistory.low.asc()).first()
                if low_record:
                    return low_record.low
                else:
                    return None
        if field == "percent_change":
            with get_db() as db:
                # find the last close that occurred during 'yesterday' (UTC)
                today_utc = datetime.utcnow().date()
                start_of_today = datetime(today_utc.year, today_utc.month, today_utc.day)
                start_of_yesterday = start_of_today - timedelta(days=1)

                last_record_yesterday = db.query(StockHistory).filter(
                    StockHistory.stock_symbol == stock.symbol,
                    StockHistory.is_hourly == False,
                    StockHistory.day_and_time >= start_of_yesterday,
                    StockHistory.day_and_time < start_of_today,
                ).order_by(StockHistory.day_and_time.desc()).first()

                latest_record = db.query(StockHistory).filter(
                    StockHistory.stock_symbol == stock.symbol,
                    StockHistory.is_hourly == False
                ).order_by(StockHistory.day_and_time.desc()).first()

                if not last_record_yesterday:
                    return None

                last_close = last_record_yesterday.close_price

                # determine current price (prefer stock.price if present)
                current_price = getattr(stock, 'price', None)
                if current_price is None and latest_record:
                    current_price = latest_record.close_price

                if current_price is None or last_close in (0, None):
                    return None

                return ((current_price - last_close) / last_close) * 100

    def get_historical_data(self, ticker, from_date, to_date, timespan='day', multiplier=1, Stock: Stock = None):
        """Fetch historical data from Polygon API."""
        client = RESTClient(api_key=API_KEY)

        aggs = client.list_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_date,
            to=to_date,
            adjusted=True,
            sort="asc"
        )

        data = []
        for bar in aggs:
            data.append({
                'timestamp': datetime.fromtimestamp(bar.timestamp/1000),
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume
            })

        df = pd.DataFrame(data)
        if not df.empty:
            df.set_index('timestamp', inplace=True)
        return df

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
        
        
        logger.info(f"Fetching data for {len(tickers)} tickers:")
        
        results = {}
        failed_tickers = []
        total_rows_inserted = 0
        
        for idx, ticker in enumerate(tickers, 1):
            try:
                stock = None
                with get_db() as db:
                    stock = db.execute(select(Stock).where(Stock.symbol == ticker)).first()
                logger.info(f"Processing {ticker} ({idx}/{len(tickers)})")
                ticker_rows = 0
                start_date = end_date - timedelta(days=730)
                if(stock is not None and stock.updated_at is not None):
                    start_date = stock.updated_at
                
                logger.info(f"  Fetching 2 years of hourly data for {ticker}...")
                hourly_df = self.get_historical_data(ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), timespan='hour', multiplier=1, Stock=stock)
                if not hourly_df.empty:
                    rows = self.db_service.save_stock_data_to_db(ticker, hourly_df, is_hourly=True)
                    ticker_rows += rows
                    logger.info(f"  Saved {rows} hourly rows for {ticker}")
                minute_start_date = end_date - timedelta(days=28)
                if(stock is not None and stock.updated_at is not None):
                    minute_start_date = stock.updated_at

                logger.info(f"  Fetching 1 month of minute data for {ticker}...")
                minute_df = self.get_historical_data(ticker, minute_start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), timespan='minute', multiplier=1, Stock=stock)
                if not minute_df.empty:
                    rows = self.db_service.save_stock_data_to_db(ticker, minute_df, is_hourly=False)
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
                        "error": "No data returned from Polygon"
                    }
                    failed_tickers.append(ticker)
                    logger.error(f"✗✗✗ FAILED: {ticker} - No data fetched from Polygon")

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
                        df = self.get_historical_data(ticker, gap_start.strftime('%Y-%m-%d'), gap_end.strftime('%Y-%m-%d'), timespan='hour', multiplier=1)
                    else:
                        df = self.get_historical_data(ticker, gap_start.strftime('%Y-%m-%d'), gap_end.strftime('%Y-%m-%d'), timespan='minute', multiplier=1)
                    
                    if not df.empty:
                        rows_inserted = self.db_service.save_stock_data_to_db(ticker, df, is_hourly=is_hourly)
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
                            time.sleep(3)
                        else:
                            logger.warning(f"✗ Could not fill gap after {max_retries} retries - adding to blacklist")
                    
                except Exception as e:
                    retry_count += 1
                    logger.error(f"Error filling gap for {ticker} ({gap_start} to {gap_end}): {str(e)}")
                    if retry_count < max_retries:
                        logger.info(f"Retrying... ({retry_count}/{max_retries})")
                        time.sleep(3)

            if not gap_filled:
                try:
                    self.db_service.add_to_blacklist(ticker, gap_start, is_hourly=is_hourly)
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

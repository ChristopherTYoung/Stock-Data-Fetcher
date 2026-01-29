"""
Gap detector for identifying missing data in stock history.
"""
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from sqlalchemy import select, and_, or_
from database import get_db, StockHistory, Stock, Blacklist
import logging

logger = logging.getLogger(__name__)


class GapDetector:
    """Detects gaps in minutely and hourly stock data."""
    
    def __init__(self, blacklist_expiration_time: int = 24):
        """
        Initialize the gap detector.
        
        Args:
            blacklist_expiration_time: Time in hours before a blacklisted stock can be checked again
        """
        self.blacklist_expiration_time = blacklist_expiration_time
        # Store model references for testability
        self.Stock = Stock
        self.StockHistory = StockHistory
        self.Blacklist = Blacklist
    
    def check_for_gaps(self, symbol: str) -> List[Tuple[datetime, datetime, bool]]:
        """
        Check the database for gaps in minutely or hourly data for a given stock symbol.
        
        Args:
            symbol: Stock symbol to check for gaps
            
        Returns:
            List of tuples containing (gap_start, gap_end, is_hourly) for each gap found
        """
        gaps = []
        
        with get_db() as db:
            # First check if the stock exists
            stock_result = db.execute(select(self.Stock).where(self.Stock.symbol == symbol)).first()
            if not stock_result:
                logger.warning(f"Stock {symbol} not found in database")
                return gaps
            
            # Check for gaps in hourly data (should have 2 years of data)
            hourly_gaps = self._check_hourly_gaps(db, symbol)
            gaps.extend(hourly_gaps)
            
            # Check for gaps in minute data (should have 1 month of data)
            minute_gaps = self._check_minute_gaps(db, symbol)
            gaps.extend(minute_gaps)
            
            # Filter gaps against blacklist
            gaps = self._filter_blacklisted_gaps(db, symbol, gaps)
        
        if gaps:
            logger.info(f"Found {len(gaps)} gaps for {symbol}")
        else:
            logger.info(f"No gaps found for {symbol}")
        
        return gaps
    
    def _filter_blacklisted_gaps(self, db, symbol: str, gaps: List[Tuple[datetime, datetime, bool]]) -> List[Tuple[datetime, datetime, bool]]:
        """
        Filter out gaps that are in the blacklist and still within the expiration time.
        
        Args:
            db: Database session
            symbol: Stock symbol
            gaps: List of gaps to filter
            
        Returns:
            Filtered list of gaps with blacklisted (non-expired) gaps removed
        """
        if not gaps:
            return gaps

        blacklist_entries = db.execute(
            select(self.Blacklist.timestamp, self.Blacklist.time_added)
            .where(self.Blacklist.stock_symbol == symbol)
        ).fetchall()
        
        if not blacklist_entries:
            return gaps

        expiration_cutoff = datetime.now() - timedelta(hours=self.blacklist_expiration_time)
        
        active_blacklist = {
            entry[0].replace(microsecond=0) for entry in blacklist_entries
            if entry[1] >= expiration_cutoff
        }

        filtered_gaps = []
        for gap_start, gap_end, is_hourly in gaps:
            normalized_gap_start = gap_start.replace(microsecond=0)
            
            if normalized_gap_start not in active_blacklist:
                filtered_gaps.append((gap_start, gap_end, is_hourly))
            else:
                logger.info(f"Gap filtered by blacklist: {symbol} from {gap_start} to {gap_end}")
        
        return filtered_gaps
    
    def _check_hourly_gaps(self, db, symbol: str) -> List[Tuple[datetime, datetime, bool]]:
        """
        Check for gaps in hourly data (should cover last 2 years).
        
        Args:
            db: Database session
            symbol: Stock symbol
            
        Returns:
            List of gaps in format (gap_start, gap_end, True) where True indicates hourly data
        """
        gaps = []
        
        hourly_data = db.execute(
            select(self.StockHistory.day_and_time)
            .where(and_(
                self.StockHistory.stock_symbol == symbol,
                self.StockHistory.is_hourly == True
            ))
            .order_by(self.StockHistory.day_and_time)
        ).fetchall()
        
        if not hourly_data:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=730)  # 2 years
            gaps.append((start_time, end_time, True))
            logger.info(f"No hourly data found for {symbol}, gap from {start_time} to {end_time}")
            return gaps

        for i in range(len(hourly_data) - 1):
            current_time = hourly_data[i][0]
            next_time = hourly_data[i + 1][0]

            time_diff = next_time - current_time

            if time_diff > timedelta(days=7):
                gaps.append((current_time, next_time, True))
                logger.info(f"Hourly gap found for {symbol}: {current_time} to {next_time}")

        oldest_time = hourly_data[0][0]
        two_years_ago = datetime.now() - timedelta(days=730)
        if oldest_time > two_years_ago:
            gaps.append((two_years_ago, oldest_time, True))
            logger.info(f"Historical hourly gap for {symbol}: {two_years_ago} to {oldest_time}")

        newest_time = hourly_data[-1][0]
        one_week_ago = datetime.now() - timedelta(days=7)
        if newest_time < one_week_ago:
            gaps.append((newest_time, datetime.now(), True))
            logger.info(f"Recent hourly gap for {symbol}: {newest_time} to now")
        
        return gaps
    
    def _check_minute_gaps(self, db, symbol: str) -> List[Tuple[datetime, datetime, bool]]:
        """
        Check for gaps in minute data (should cover last 30 days).
        
        Args:
            db: Database session
            symbol: Stock symbol
            
        Returns:
            List of gaps in format (gap_start, gap_end, False) where False indicates minute data
        """
        gaps = []

        minute_data = db.execute(
            select(self.StockHistory.day_and_time)
            .where(and_(
                self.StockHistory.stock_symbol == symbol,
                self.StockHistory.is_hourly == False
            ))
            .order_by(self.StockHistory.day_and_time)
        ).fetchall()
        
        if not minute_data:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=30)
            gaps.append((start_time, end_time, False))
            logger.info(f"No minute data found for {symbol}, gap from {start_time} to {end_time}")
            return gaps

        for i in range(len(minute_data) - 1):
            current_time = minute_data[i][0]
            next_time = minute_data[i + 1][0]
            
            time_diff = next_time - current_time

            if time_diff > timedelta(days=1):
                gaps.append((current_time, next_time, False))
                logger.info(f"Minute gap found for {symbol}: {current_time} to {next_time}")

        oldest_time = minute_data[0][0]
        thirty_days_ago = datetime.now() - timedelta(days=30)
        if oldest_time > thirty_days_ago:
            gaps.append((thirty_days_ago, oldest_time, False))
            logger.info(f"Historical minute gap for {symbol}: {thirty_days_ago} to {oldest_time}")

        newest_time = minute_data[-1][0]
        one_day_ago = datetime.now() - timedelta(days=1)
        if newest_time < one_day_ago:
            gaps.append((newest_time, datetime.now(), False))
            logger.info(f"Recent minute gap for {symbol}: {newest_time} to now")
        
        return gaps

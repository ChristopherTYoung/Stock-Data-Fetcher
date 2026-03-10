"""Stock calculation utilities for computing price, 52-week highs/lows, and percent change."""
from datetime import datetime, timedelta
from typing import Any, Optional
import pandas as pd
import logging
from database import get_db, StockHistory, Stock

logger = logging.getLogger(__name__)


class StockCalculator:
    """Static methods for calculating stock metrics from historical and DB data."""

    @staticmethod
    def prepare_combined_dataframe(history_data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Combine history_data with DB rows from the past year for accurate 52-week metrics.
        
        Returns combined DataFrame with history_data taking precedence for duplicate timestamps.
        """
        combined = history_data.copy() if isinstance(history_data, pd.DataFrame) else history_data
        
        if symbol is not None and isinstance(combined, pd.DataFrame) and not combined.empty:
            try:
                with get_db() as db:
                    one_year_ago = datetime.utcnow() - timedelta(days=365)
                    rows = db.query(StockHistory).filter(
                        StockHistory.stock_symbol == symbol,
                        StockHistory.is_hourly == False,
                        StockHistory.day_and_time >= one_year_ago
                    ).order_by(StockHistory.day_and_time.asc()).all()
                    
                    if rows:
                        db_data_rows = []
                        for r in rows:
                            db_data_rows.append({
                                'timestamp': r.day_and_time,
                                'open': r.open_price,
                                'high': r.high,
                                'low': r.low,
                                'close': r.close_price,
                                'volume': r.volume
                            })
                        db_df = pd.DataFrame(db_data_rows)
                        if not db_df.empty:
                            db_df['timestamp'] = pd.to_datetime(db_df['timestamp'])
                            db_df.set_index('timestamp', inplace=True)
                            combined = pd.concat([db_df, combined])
                            combined = combined[~combined.index.duplicated(keep='last')]
                            combined.sort_index(inplace=True)
            except Exception as e:
                logger.warning(f"Failed to fetch DB data for {symbol}, proceeding with history_data only: {e}")
        
        return combined

    @staticmethod
    def calculate_price(history_data: Optional[pd.DataFrame], stock: Optional[Stock]) -> Any:
        """Calculate the latest close price. Always prefers history_data if available."""
        if isinstance(history_data, pd.DataFrame) and not history_data.empty:
            try:
                df = history_data.copy()
                if not isinstance(df.index, pd.DatetimeIndex):
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.set_index('timestamp', inplace=True)
                    else:
                        df.index = pd.to_datetime(df.index)
                return df['close'].iloc[-1]
            except Exception:
                pass
        
        # Fallback to DB
        if stock and hasattr(stock, 'symbol'):
            with get_db() as db:
                latest_record = db.query(StockHistory).filter(
                    StockHistory.stock_symbol == stock.symbol,
                    StockHistory.is_hourly == False
                ).order_by(StockHistory.day_and_time.desc()).first()
                if latest_record:
                    return latest_record.close_price
        
        return None

    @staticmethod
    def calculate_high52(history_data: Optional[pd.DataFrame], stock: Optional[Stock], symbol: Optional[str]) -> Any:
        """Calculate 52-week high by combining history_data with DB rows."""
        df = history_data.copy() if isinstance(history_data, pd.DataFrame) else None
        
        if df is not None and not df.empty:
            # Ensure proper datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                else:
                    df.index = pd.to_datetime(df.index)
            
            # Combine with DB rows for past year
            combined = StockCalculator.prepare_combined_dataframe(df, symbol)
            
            if not combined.empty:
                idx_max = combined.index.max()
                one_year_ago = idx_max - pd.Timedelta(days=365)
                subset = combined[combined.index >= one_year_ago]
                if not subset.empty:
                    return subset['high'].max()
        
        # Fallback to DB only
        if stock and hasattr(stock, 'symbol'):
            with get_db() as db:
                one_year_ago = datetime.utcnow() - timedelta(days=365)
                high_record = db.query(StockHistory).filter(
                    StockHistory.stock_symbol == stock.symbol,
                    StockHistory.is_hourly == False,
                    StockHistory.day_and_time >= one_year_ago
                ).order_by(StockHistory.high.desc()).first()
                if high_record:
                    return high_record.high
        
        return None

    @staticmethod
    def calculate_low52(history_data: Optional[pd.DataFrame], stock: Optional[Stock], symbol: Optional[str]) -> Any:
        """Calculate 52-week low by combining history_data with DB rows."""
        df = history_data.copy() if isinstance(history_data, pd.DataFrame) else None
        
        if df is not None and not df.empty:
            # Ensure proper datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                else:
                    df.index = pd.to_datetime(df.index)
            
            # Combine with DB rows for past year
            combined = StockCalculator.prepare_combined_dataframe(df, symbol)
            
            if not combined.empty:
                idx_max = combined.index.max()
                one_year_ago = idx_max - pd.Timedelta(days=365)
                subset = combined[combined.index >= one_year_ago]
                if not subset.empty:
                    return subset['low'].min()
        
        # Fallback to DB only
        if stock and hasattr(stock, 'symbol'):
            with get_db() as db:
                one_year_ago = datetime.utcnow() - timedelta(days=365)
                low_record = db.query(StockHistory).filter(
                    StockHistory.stock_symbol == stock.symbol,
                    StockHistory.is_hourly == False,
                    StockHistory.day_and_time >= one_year_ago
                ).order_by(StockHistory.low.asc()).first()
                if low_record:
                    return low_record.low
        
        return None

    @staticmethod
    def calculate_percent_change(history_data: Optional[pd.DataFrame], stock: Optional[Stock], symbol: Optional[str]) -> Any:
        """Calculate percent change from yesterday by combining history_data with DB rows."""
        df = history_data.copy() if isinstance(history_data, pd.DataFrame) else None
        
        if df is not None and not df.empty:
            # Ensure proper datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                else:
                    df.index = pd.to_datetime(df.index)
            
            # Combine with DB rows for past year
            combined = StockCalculator.prepare_combined_dataframe(df, symbol)
            
            if not combined.empty:
                idx_max = combined.index.max()
                start_of_today = datetime(idx_max.year, idx_max.month, idx_max.day)
                start_of_yesterday = start_of_today - timedelta(days=1)
                yesterday_rows = combined[(combined.index >= start_of_yesterday) & (combined.index < start_of_today)]
                
                if not yesterday_rows.empty:
                    last_close = yesterday_rows['close'].iloc[-1]
                    current_price = combined['close'].iloc[-1]
                    if current_price is not None and last_close not in (0, None):
                        return ((current_price - last_close) / last_close) * 100
        
        # Fallback to DB only
        if stock and hasattr(stock, 'symbol'):
            with get_db() as db:
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

                if last_record_yesterday:
                    last_close = last_record_yesterday.close_price
                    current_price = getattr(stock, 'price', None)
                    if current_price is None and latest_record:
                        current_price = latest_record.close_price
                    
                    if current_price is not None and last_close not in (0, None):
                        return ((current_price - last_close) / last_close) * 100
        
        return None

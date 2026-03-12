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
        logger.debug(f"Preparing combined dataframe for symbol={symbol}, history_data shape={history_data.shape if isinstance(history_data, pd.DataFrame) else 'N/A'}")
        
        combined = history_data.copy() if isinstance(history_data, pd.DataFrame) else history_data
        
        if symbol is not None and isinstance(combined, pd.DataFrame) and not combined.empty:
            try:
                logger.debug(f"Fetching historical data from DB for {symbol} from past year")
                with get_db() as db:
                    one_year_ago = datetime.utcnow() - timedelta(days=365)
                    rows = db.query(StockHistory).filter(
                        StockHistory.stock_symbol == symbol,
                        StockHistory.is_hourly == False,
                        StockHistory.day_and_time >= one_year_ago
                    ).order_by(StockHistory.day_and_time.asc()).all()
                    
                    logger.debug(f"Retrieved {len(rows) if rows else 0} rows from DB for {symbol}")
                    
                    if rows:
                        db_data_rows = []
                        for r in rows:
                            db_data_rows.append({
                                'timestamp': r.day_and_time,
                                'open': r.open_price / 100,
                                'high': r.high / 100,
                                'low': r.low / 100,
                                'close': r.close_price / 100,
                                'volume': r.volume
                            })
                        db_df = pd.DataFrame(db_data_rows)
                        if not db_df.empty:
                            db_df['timestamp'] = pd.to_datetime(db_df['timestamp'])
                            db_df.set_index('timestamp', inplace=True)
                            logger.debug(f"DB dataframe shape before concat: {db_df.shape}, history_data shape: {combined.shape}")
                            combined = pd.concat([db_df, combined])
                            combined = combined[~combined.index.duplicated(keep='last')]
                            combined.sort_index(inplace=True)
                            logger.debug(f"Combined dataframe shape after deduplication and sorting: {combined.shape}")
                    else:
                        logger.debug(f"No historical data found in DB for {symbol}, using history_data only")
            except Exception as e:
                logger.warning(f"Failed to fetch DB data for {symbol}, proceeding with history_data only. Error: {type(e).__name__}: {e}", exc_info=True)
        else:
            logger.debug(f"Skipping DB fetch: symbol={symbol}, is_dataframe={isinstance(combined, pd.DataFrame)}, is_empty={combined.empty if isinstance(combined, pd.DataFrame) else 'N/A'}")
        
        logger.debug(f"Returning combined dataframe with shape {combined.shape if isinstance(combined, pd.DataFrame) else 'N/A'}")
        return combined

    @staticmethod
    def calculate_price(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate the latest close price. Always prefers history_data if available."""
        symbol = stock.symbol if stock and hasattr(stock, 'symbol') else 'UNKNOWN'
        logger.debug(f"Calculating price for {symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}, stock available: {stock is not None}")
        
        if isinstance(history_data, pd.DataFrame) and not history_data.empty:
            try:
                df = history_data.copy()
                if not isinstance(df.index, pd.DatetimeIndex):
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.set_index('timestamp', inplace=True)
                    else:
                        df.index = pd.to_datetime(df.index)
                price = df['close'].iloc[-1]
                logger.debug(f"Calculated price from history_data for {symbol}: {price}")
                return price
            except Exception as e:
                logger.warning(f"Failed to calculate price from history_data for {symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        # Fallback to DB
        if stock and hasattr(stock, 'symbol'):
            logger.debug(f"Falling back to DB lookup for price of {stock.symbol}")
            try:
                with get_db() as db:
                    latest_record = db.query(StockHistory).filter(
                        StockHistory.stock_symbol == stock.symbol,
                        StockHistory.is_hourly == False
                    ).order_by(StockHistory.day_and_time.desc()).first()
                    if latest_record:
                        logger.debug(f"Retrieved price from DB for {stock.symbol}: {latest_record.close_price}")
                        return latest_record.close_price / 100
                    else:
                        logger.warning(f"No price records found in DB for {stock.symbol}")
            except Exception as e:
                logger.error(f"Error querying DB for price of {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        logger.warning(f"Unable to calculate price for {symbol} - no data available")
        return None

    @staticmethod
    def calculate_high52(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate 52-week high by combining history_data with DB rows."""
        logger.debug(f"Calculating 52-week high for symbol={stock.symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}, stock available: {stock is not None}")
        
        df = history_data.copy() if isinstance(history_data, pd.DataFrame) else None
        
        if df is not None and not df.empty:
            try:
                # Ensure proper datetime index
                if not isinstance(df.index, pd.DatetimeIndex):
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.set_index('timestamp', inplace=True)
                    else:
                        df.index = pd.to_datetime(df.index)
                
                logger.debug(f"Converting history_data datetime index for {stock.symbol}")
                # Combine with DB rows for past year
                combined = StockCalculator.prepare_combined_dataframe(df, stock.symbol)
                
                if not combined.empty:
                    idx_max = combined.index.max()
                    one_year_ago = idx_max - pd.Timedelta(days=365)
                    subset = combined[combined.index >= one_year_ago]
                    logger.debug(f"52-week subset for {stock.symbol}: {len(subset)} records from {one_year_ago.date()} to {idx_max.date()}")
                    if not subset.empty:
                        high_52 = subset['high'].max()
                        logger.debug(f"52-week high for {stock.symbol}: {high_52}")
                        return high_52
                    else:
                        logger.warning(f"No data found in 52-week window for {stock.symbol}")
                else:
                    logger.warning(f"Combined dataframe is empty for {stock.symbol}")
            except Exception as e:
                logger.warning(f"Failed to calculate 52-week high from history_data for {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        # Fallback to DB only
        if stock and hasattr(stock, 'symbol'):
            logger.debug(f"Falling back to DB-only lookup for 52-week high of {stock.symbol}")
            try:
                with get_db() as db:
                    one_year_ago = datetime.utcnow() - timedelta(days=365)
                    high_record = db.query(StockHistory).filter(
                        StockHistory.stock_symbol == stock.symbol,
                        StockHistory.is_hourly == False,
                        StockHistory.day_and_time >= one_year_ago
                    ).order_by(StockHistory.high.desc()).first()
                    if high_record:
                        logger.debug(f"Retrieved 52-week high from DB for {stock.symbol}: {high_record.high}")
                        return high_record.high / 100
                    else:
                        logger.warning(f"No 52-week high records found in DB for {stock.symbol}")
            except Exception as e:
                logger.error(f"Error querying DB for 52-week high of {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        logger.warning(f"Unable to calculate 52-week high for {stock.symbol} - no data available")
        return None

    @staticmethod
    def calculate_low52(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate 52-week low by combining history_data with DB rows."""
        logger.debug(f"Calculating 52-week low for symbol={stock.symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}, stock available: {stock is not None}")
        
        df = history_data.copy() if isinstance(history_data, pd.DataFrame) else None
        
        if df is not None and not df.empty:
            try:
                # Ensure proper datetime index
                if not isinstance(df.index, pd.DatetimeIndex):
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.set_index('timestamp', inplace=True)
                    else:
                        df.index = pd.to_datetime(df.index)
                
                logger.debug(f"Converting history_data datetime index for {stock.symbol}")
                # Combine with DB rows for past year
                combined = StockCalculator.prepare_combined_dataframe(df, stock.symbol)
                
                if not combined.empty:
                    idx_max = combined.index.max()
                    one_year_ago = idx_max - pd.Timedelta(days=365)
                    subset = combined[combined.index >= one_year_ago]
                    logger.debug(f"52-week subset for {stock.symbol}: {len(subset)} records from {one_year_ago.date()} to {idx_max.date()}")
                    if not subset.empty:
                        low_52 = subset['low'].min()
                        logger.debug(f"52-week low for {stock.symbol}: {low_52}")
                        return low_52
                    else:
                        logger.warning(f"No data found in 52-week window for {stock.symbol}")
                else:
                    logger.warning(f"Combined dataframe is empty for {stock.symbol}")
            except Exception as e:
                logger.warning(f"Failed to calculate 52-week low from history_data for {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        # Fallback to DB only
        if stock and hasattr(stock, 'symbol'):
            logger.debug(f"Falling back to DB-only lookup for 52-week low of {stock.symbol}")
            try:
                with get_db() as db:
                    one_year_ago = datetime.utcnow() - timedelta(days=365)
                    low_record = db.query(StockHistory).filter(
                        StockHistory.stock_symbol == stock.symbol,
                        StockHistory.is_hourly == False,
                        StockHistory.day_and_time >= one_year_ago
                    ).order_by(StockHistory.low.asc()).first()
                    if low_record:
                        logger.debug(f"Retrieved 52-week low from DB for {stock.symbol}: {low_record.low}")
                        return low_record.low / 100
                    else:
                        logger.warning(f"No 52-week low records found in DB for {stock.symbol}")
            except Exception as e:
                logger.error(f"Error querying DB for 52-week low of {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        logger.warning(f"Unable to calculate 52-week low for {stock.symbol} - no data available")
        return None

    @staticmethod
    def calculate_percent_change(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate percent change from yesterday by combining history_data with DB rows."""
        logger.debug(f"Calculating percent change for symbol={stock.symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}, stock available: {stock is not None}")
        
        df = history_data.copy() if isinstance(history_data, pd.DataFrame) else None
        
        if df is not None and not df.empty:
            try:
                # Ensure proper datetime index
                if not isinstance(df.index, pd.DatetimeIndex):
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.set_index('timestamp', inplace=True)
                    else:
                        df.index = pd.to_datetime(df.index)
                
                logger.debug(f"Converting history_data datetime index for {stock.symbol}")
                # Combine with DB rows for past year
                combined = StockCalculator.prepare_combined_dataframe(df, stock.symbol)
                
                if not combined.empty:
                    idx_max = combined.index.max()
                    start_of_today = datetime(idx_max.year, idx_max.month, idx_max.day)
                    start_of_yesterday = start_of_today - timedelta(days=1)
                    yesterday_rows = combined[(combined.index >= start_of_yesterday) & (combined.index < start_of_today)]
                    
                    logger.debug(f"Yesterday data for {stock.symbol}: {len(yesterday_rows)} records")
                    
                    if not yesterday_rows.empty:
                        last_close = yesterday_rows['close'].iloc[-1]
                        current_price = combined['close'].iloc[-1]
                        logger.debug(f"Percent change data for {stock.symbol}: yesterday_close={last_close}, current_price={current_price}")
                        
                        if current_price is not None and last_close not in (0, None):
                            percent_change = ((current_price - last_close) / last_close) * 100
                            logger.debug(f"Calculated percent change for {stock.symbol}: {percent_change:.2f}%")
                            return percent_change
                        else:
                            logger.warning(f"Invalid price data for {stock.symbol} - yesterday_close: {last_close}, current_price: {current_price}")
                    else:
                        logger.warning(f"No yesterday data found for {stock.symbol}")
                else:
                    logger.warning(f"Combined dataframe is empty for {stock.symbol}")
            except Exception as e:
                logger.warning(f"Failed to calculate percent change from history_data for {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        # Fallback to DB only
        if stock and hasattr(stock, 'symbol'):
            logger.debug(f"Falling back to DB-only lookup for percent change of {stock.symbol}")
            try:
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
                    
                    logger.debug(f"DB lookup for {stock.symbol}: yesterday_record={last_record_yesterday is not None}, latest_record={latest_record is not None}")

                    if last_record_yesterday:
                        last_close = last_record_yesterday.close_price
                        current_price = getattr(stock, 'price', None)
                        if current_price is None and latest_record:
                            current_price = latest_record.close_price
                            current_price = current_price / 100
                            logger.debug(f"Used latest_record close price: {current_price}")
                        
                        logger.debug(f"Percent change data for {stock.symbol}: yesterday_close={last_close}, current_price={current_price}")
                        
                        if current_price is not None and last_close not in (0, None):
                            last_close = last_close / 100
                            percent_change = ((current_price - last_close) / last_close) * 100
                            logger.debug(f"Calculated percent change for {stock.symbol}: {percent_change:.2f}%")
                            return percent_change
                        else:
                            logger.warning(f"Invalid price data for {stock.symbol} - yesterday_close: {last_close}, current_price: {current_price}")
                    else:
                        logger.warning(f"No yesterday data found in DB for {stock.symbol}")
            except Exception as e:
                logger.error(f"Error querying DB for percent change of {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        logger.warning(f"Unable to calculate percent change for {stock.symbol} - no data available")
        return None

    @staticmethod
    def calculate_pe(stock: Stock, price: Optional[float] = None) -> Optional[float]:
        if stock is None:
            logger.warning("Cannot calculate P/E without stock object")
            return None
        
        symbol = getattr(stock, 'symbol', 'UNKNOWN')
        logger.debug(f"Calculating P/E for {symbol}. price parameter={price}, stock.price={getattr(stock, 'price', None)}")
        
        if price is not None:
            current_price = price
            logger.debug(f"Using provided price parameter: {current_price}")
        else:
            current_price = getattr(stock, 'price', None)
            logger.debug(f"Using price from stock object: {current_price}")
        
        eps = getattr(stock, 'eps', None)
        logger.debug(f"EPS for {symbol}: {eps}")
        
        if current_price is None:
            logger.warning(f"Cannot calculate P/E for {symbol} - price is None")
            return None
        
        if eps is None:
            logger.warning(f"Cannot calculate P/E for {symbol} - EPS is None")
            return None
        
        if eps == 0:
            logger.warning(f"Cannot calculate P/E for {symbol} - EPS is zero (would cause division by zero)")
            return None
        
        try:
            pe_ratio = current_price / eps
            logger.debug(f"Calculated P/E for {symbol}: {pe_ratio:.2f} (price={current_price}, eps={eps})")
            return pe_ratio
        except Exception as e:
            logger.error(f"Error calculating P/E for {symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
            return None

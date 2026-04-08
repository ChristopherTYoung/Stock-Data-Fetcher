"""Stock calculation utilities for computing price, 52-week highs/lows, and percent change."""
from datetime import datetime, timedelta
from typing import Any, Optional
import pandas as pd
import logging
from stock_data_calculator.database import get_db, StockHistory, Stock
from stock_data_calculator.logging_config import setup_logging

logger = setup_logging("stock-data-calculator", level=logging.INFO)


class StockCalculator:
    """Static methods for calculating stock metrics from historical and DB data."""

    @staticmethod
    def calculate_price(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate the latest close price from Polygon history data only."""
        symbol = stock.symbol if stock and hasattr(stock, 'symbol') else 'UNKNOWN'
        logger.debug(f"Calculating price for {symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}")
        
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
        
        logger.warning(f"No price data available for {symbol}")
        return None

    @staticmethod
    def calculate_high52(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate 52-week high from Polygon history data only."""
        logger.debug(f"Calculating 52-week high for symbol={stock.symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}")
        
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
                combined = df
                
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
        
        logger.warning(f"Unable to calculate 52-week high for {stock.symbol} - no data available")
        return None

    @staticmethod
    def calculate_low52(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate 52-week low from Polygon history data only."""
        logger.debug(f"Calculating 52-week low for symbol={stock.symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}")
        
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
                combined = df
                
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
        
        logger.warning(f"Unable to calculate 52-week low for {stock.symbol} - no data available")
        return None

    @staticmethod
    def calculate_percent_change(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate percent change from the last available close price using Polygon history data only."""
        logger.debug(f"Calculating percent change for symbol={stock.symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}")
        
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
                combined = df
                
                if not combined.empty and len(combined) > 1:
                    current_price = combined['close'].iloc[-1]
                    previous_close = combined['close'].iloc[-2]
                    logger.debug(f"Percent change data for {stock.symbol}: previous_close={previous_close}, current_price={current_price}")
                    
                    if current_price is not None and previous_close not in (0, None):
                        percent_change = ((current_price - previous_close) / previous_close) * 100
                        logger.debug(f"Calculated percent change for {stock.symbol}: {percent_change:.2f}%")
                        return percent_change
                    else:
                        logger.warning(f"Invalid price data for {stock.symbol} - previous_close: {previous_close}, current_price: {current_price}")
                elif not combined.empty and len(combined) == 1:
                    logger.warning(f"Only one data point available for {stock.symbol} - cannot calculate percent change")
                else:
                    logger.warning(f"Combined dataframe is empty for {stock.symbol}")
            except Exception as e:
                logger.warning(f"Failed to calculate percent change from history_data for {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
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
            normalized_price = float(current_price)
            normalized_eps = float(eps)
            pe_ratio = normalized_price / normalized_eps
            logger.debug(f"Calculated P/E for {symbol}: {pe_ratio:.2f} (price={current_price}, eps={eps})")
            return pe_ratio
        except Exception as e:
            logger.error(f"Error calculating P/E for {symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
            return None

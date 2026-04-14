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
    def _normalize_history_dataframe(history_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Return a sorted copy of history data indexed by timestamp."""
        if not isinstance(history_data, pd.DataFrame) or history_data.empty:
            return None

        df = history_data.copy()
        try:
            if not isinstance(df.index, pd.DatetimeIndex):
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                else:
                    df.index = pd.to_datetime(df.index)

            df.sort_index(inplace=True)
            return df
        except Exception as e:
            logger.warning(
                f"Failed to normalize history data. Error: {type(e).__name__}: {e}",
                exc_info=True,
            )
            return None

    @staticmethod
    def calculate_price(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate the latest close price from stock history data only."""
        symbol = stock.symbol if stock and hasattr(stock, 'symbol') else 'UNKNOWN'
        logger.debug(f"Calculating price for {symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}")

        df = StockCalculator._normalize_history_dataframe(history_data)
        if df is not None:
            try:
                price = df['close'].iloc[-1]
                logger.debug(f"Calculated price from history_data for {symbol}: {price}")
                return price
            except Exception as e:
                logger.warning(f"Failed to calculate price from history_data for {symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        logger.warning(f"No price data available for {symbol}")
        return None

    @staticmethod
    def calculate_high52(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate 52-week high from stock history data only."""
        logger.debug(f"Calculating 52-week high for symbol={stock.symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}")

        df = StockCalculator._normalize_history_dataframe(history_data)

        if df is not None:
            try:
                idx_max = df.index.max()
                one_year_ago = idx_max - pd.Timedelta(days=365)
                subset = df[df.index >= one_year_ago]
                logger.debug(f"52-week subset for {stock.symbol}: {len(subset)} records from {one_year_ago.date()} to {idx_max.date()}")
                if not subset.empty:
                    high_52 = subset['high'].max()
                    logger.debug(f"52-week high for {stock.symbol}: {high_52}")
                    return high_52
                else:
                    logger.warning(f"No data found in 52-week window for {stock.symbol}")
            except Exception as e:
                logger.warning(f"Failed to calculate 52-week high from history_data for {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        logger.warning(f"Unable to calculate 52-week high for {stock.symbol} - no data available")
        return None

    @staticmethod
    def calculate_low52(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate 52-week low from stock history data only."""
        logger.debug(f"Calculating 52-week low for symbol={stock.symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}")

        df = StockCalculator._normalize_history_dataframe(history_data)

        if df is not None:
            try:
                idx_max = df.index.max()
                one_year_ago = idx_max - pd.Timedelta(days=365)
                subset = df[df.index >= one_year_ago]
                logger.debug(f"52-week subset for {stock.symbol}: {len(subset)} records from {one_year_ago.date()} to {idx_max.date()}")
                if not subset.empty:
                    low_52 = subset['low'].min()
                    logger.debug(f"52-week low for {stock.symbol}: {low_52}")
                    return low_52
                else:
                    logger.warning(f"No data found in 52-week window for {stock.symbol}")
            except Exception as e:
                logger.warning(f"Failed to calculate 52-week low from history_data for {stock.symbol}. Error: {type(e).__name__}: {e}", exc_info=True)
        
        logger.warning(f"Unable to calculate 52-week low for {stock.symbol} - no data available")
        return None

    @staticmethod
    def calculate_percent_change(history_data: pd.DataFrame, stock: Stock) -> Any:
        """Calculate day-over-day percent change using the last close from the last two trading dates."""
        logger.debug(f"Calculating percent change for symbol={stock.symbol}. history_data available: {isinstance(history_data, pd.DataFrame)}")

        df = StockCalculator._normalize_history_dataframe(history_data)

        if df is not None:
            try:
                daily_closes = df.groupby(df.index.normalize())['close'].last()

                if len(daily_closes) > 1:
                    previous_close = daily_closes.iloc[-2]
                    current_price = daily_closes.iloc[-1]
                    logger.debug(
                        f"Percent change data for {stock.symbol}: previous_close={previous_close}, current_price={current_price}"
                    )

                    if current_price is not None and previous_close not in (0, None):
                        percent_change = ((current_price - previous_close) / previous_close) * 100
                        logger.debug(f"Calculated percent change for {stock.symbol}: {percent_change:.2f}%")
                        return percent_change
                    else:
                        logger.warning(
                            f"Invalid price data for {stock.symbol} - previous_close: {previous_close}, current_price: {current_price}"
                        )
                elif len(daily_closes) == 1:
                    logger.warning(
                        f"Only one trading day available for {stock.symbol} - cannot calculate day-over-day percent change"
                    )
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

    @staticmethod
    def calculate_last_candlestick_pattern(history_data: pd.DataFrame, stock: Stock) -> Optional[str]:
        """
        Detect the candlestick pattern of the last candle in the history data.
        Returns the pattern name ('hammer', 'inverted_hammer', 'hanging_man') or None.
        """
        symbol = stock.symbol if stock and hasattr(stock, 'symbol') else 'UNKNOWN'
        
        df = StockCalculator._normalize_history_dataframe(history_data)

        if df is None:
            logger.debug(f"No history data available for candlestick pattern detection for {symbol}")
            return None
        
        try:
            # Get the last row (most recent candle)
            last_candle = df.iloc[-1]
            
            # Extract OHLC values - handle both Series attributes and dict-like access
            open_price = last_candle['open'] if 'open' in df.columns else getattr(last_candle, 'open', None)
            close_price = last_candle['close'] if 'close' in df.columns else getattr(last_candle, 'close', None)
            high = last_candle['high'] if 'high' in df.columns else getattr(last_candle, 'high', None)
            low = last_candle['low'] if 'low' in df.columns else getattr(last_candle, 'low', None)
            
            if any(x is None for x in [open_price, close_price, high, low]):
                logger.debug(f"Incomplete OHLC data for {symbol}: open={open_price}, close={close_price}, high={high}, low={low}")
                return None
            
            # Convert to float to ensure numeric operations work
            open_price = float(open_price)
            close_price = float(close_price)
            high = float(high)
            low = float(low)
            
            # Detect pattern
            pattern = CandlestickPatternDetector.detect_last_candle(
                open_price, close_price, high, low
            )
            
            if pattern:
                logger.debug(f"Detected candlestick pattern for {symbol}: {pattern}")
            
            return pattern
            
        except Exception as e:
            logger.warning(f"Error detecting candlestick pattern for {symbol}: {type(e).__name__}: {e}")
            return None


class CandlestickPatternDetector:
    """Detects candlestick patterns based on proportion thresholds."""
    
    # Pattern thresholds
    HAMMER_BODY_MAX = 0.25
    HAMMER_LOWER_WICK_MIN_RATIO = 2.0
    HAMMER_UPPER_WICK_MAX = 0.25
    
    INVERTED_HAMMER_BODY_MAX = 0.25
    INVERTED_HAMMER_UPPER_WICK_MIN_RATIO = 2.0
    INVERTED_HAMMER_LOWER_WICK_MAX = 0.25
    
    HANGING_MAN_BODY_MAX = 0.25
    HANGING_MAN_LOWER_WICK_MIN_RATIO = 2.0
    HANGING_MAN_UPPER_WICK_MAX = 0.25
    
    @staticmethod
    def detect_hammer(open_price: float, close_price: float, high: float, low: float) -> bool:
        """
        Detect hammer pattern:
        - Small body at the top (close > open, bullish)
        - Long lower wick (at least 2x the body)
        - Little to no upper wick
        """
        total_size = high - low
        if total_size == 0:
            return False
        
        # Hammer must be bullish
        if close_price <= open_price:
            return False
        
        body_size = close_price - open_price
        lower_wick = open_price - low
        upper_wick = high - close_price
        
        body_ratio = body_size / total_size
        upper_wick_ratio = upper_wick / total_size
        
        wick_to_body_ratio = lower_wick / body_size if body_size > 0 else 0
        
        return (
            body_ratio <= CandlestickPatternDetector.HAMMER_BODY_MAX and
            wick_to_body_ratio >= CandlestickPatternDetector.HAMMER_LOWER_WICK_MIN_RATIO and
            upper_wick_ratio <= CandlestickPatternDetector.HAMMER_UPPER_WICK_MAX
        )
    
    @staticmethod
    def detect_inverted_hammer(open_price: float, close_price: float, high: float, low: float) -> bool:
        """
        Detect inverted hammer pattern:
        - Small body at the bottom
        - Long upper wick (at least 2x the body)
        - Little to no lower wick
        """
        total_size = high - low
        if total_size == 0:
            return False
        
        body_size = abs(close_price - open_price)
        lower_wick = min(open_price, close_price) - low
        upper_wick = high - max(open_price, close_price)
        
        body_ratio = body_size / total_size
        lower_wick_ratio = lower_wick / total_size
        
        wick_to_body_ratio = upper_wick / body_size if body_size > 0 else 0
        
        return (
            body_ratio <= CandlestickPatternDetector.INVERTED_HAMMER_BODY_MAX and
            wick_to_body_ratio >= CandlestickPatternDetector.INVERTED_HAMMER_UPPER_WICK_MIN_RATIO and
            lower_wick_ratio <= CandlestickPatternDetector.INVERTED_HAMMER_LOWER_WICK_MAX
        )
    
    @staticmethod
    def detect_hanging_man(open_price: float, close_price: float, high: float, low: float) -> bool:
        """
        Detect hanging man pattern:
        - Small body at the top (close < open, bearish)
        - Long lower wick (at least 2x the body)
        - Little to no upper wick
        """
        total_size = high - low
        if total_size == 0:
            return False
        
        # Hanging man must be bearish
        if close_price >= open_price:
            return False
        
        body_size = open_price - close_price
        lower_wick = close_price - low
        upper_wick = high - open_price
        
        body_ratio = body_size / total_size
        upper_wick_ratio = upper_wick / total_size
        
        wick_to_body_ratio = lower_wick / body_size if body_size > 0 else 0
        
        return (
            body_ratio <= CandlestickPatternDetector.HANGING_MAN_BODY_MAX and
            wick_to_body_ratio >= CandlestickPatternDetector.HANGING_MAN_LOWER_WICK_MIN_RATIO and
            upper_wick_ratio <= CandlestickPatternDetector.HANGING_MAN_UPPER_WICK_MAX
        )
    
    @staticmethod
    def detect_last_candle(open_price: float, close_price: float, high: float, low: float) -> Optional[str]:
        """
        Identify which pattern the candle matches.
        Returns the pattern name or None if no pattern matches.
        """
        if CandlestickPatternDetector.detect_hammer(open_price, close_price, high, low):
            return "hammer"
        elif CandlestickPatternDetector.detect_inverted_hammer(open_price, close_price, high, low):
            return "inverted_hammer"
        elif CandlestickPatternDetector.detect_hanging_man(open_price, close_price, high, low):
            return "hanging_man"
        return None

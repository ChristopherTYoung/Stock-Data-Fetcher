from datetime import datetime, timedelta
from pandas import DataFrame
from database import get_db, Stock, StockHistory
import os
import logging
from data_fetcher import DataFetcher
from database_service import DatabaseService
logger = logging.getLogger()

def get_history_data_from_polygon(symbol: str, start, isHourly) -> DataFrame:
    fetcher = DataFetcher(3)
    try:
        value = "hour"
        if not isHourly:
            value = "minute"
        history = fetcher.get_historical_data(symbol, start, datetime.utcnow(), value)
        return history
    except Exception as e:
        print(f"ERROR fetching data for {symbol} from Polygon: {e}")
        return {}
    
def update_stock_data(ticker: str, is_hourly: bool = False):
    with get_db() as db:
        # Get latest date for this ticker at this frequency
        latest_record = db.query(StockHistory).filter(
            StockHistory.stock_symbol == ticker.upper(),
            StockHistory.is_hourly == is_hourly
        ).order_by(StockHistory.day_and_time.desc()).first()
        if latest_record:
            latest_date = latest_record.day_and_time
        else:
            # Default to 1 year ago if no data exists
            latest_date = datetime.utcnow() - timedelta(days=365)
        logger.info(f"getting data for date {latest_date}")
        
        symbol = ticker.upper()
        api_key = os.getenv("POLYGON_API_KEY")
        history = get_history_data_from_polygon(symbol, latest_date.strftime("%Y-%m-%d"), is_hourly)
        
        logger.info(f"found stock history of length {len(history)}")
        db = DatabaseService()
        db.save_stock_data_to_db(ticker, history, is_hourly)
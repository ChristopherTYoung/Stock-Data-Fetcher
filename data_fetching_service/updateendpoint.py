from polygon import RESTClient
from datetime import datetime, timedelta
from pandas import DataFrame
from database import get_db, Stock, StockHistory
import os

def get_history_data_from_polygon(symbol: str, api_key: str, start: str) -> dict:
    client = RESTClient(api_key)
    try:
        ticker = client.get_ticker(symbol)
        history = client.history(symbol, start=start, end=datetime.utcnow().strftime('%Y-%m-%d'))  
        client.close()
        return history._raw
    except Exception as e:
        print(f"ERROR fetching data for {symbol} from Polygon: {e}")
        return {}
    
def update_stock_data(ticker: str, is_hourly: bool = False):
    db = get_db()
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
    
    symbol = ticker.upper()
    api_key = os.getenv("POLYGON_API_KEY")
    history = get_history_data_from_polygon(symbol, api_key, '2020-01-01')
    
    if history and 'results' in history:
        df = DataFrame(history['results'])
        for _, row in df.iterrows():
            timestamp = datetime.utcfromtimestamp(row['t'] / 1000)
            
            # Skip if already in database
            if timestamp <= latest_date:
                continue
            
            # Convert prices from raw to cents (multiply by 100)
            stock_history = StockHistory(
                stock_symbol=symbol,
                day_and_time=timestamp,
                open_price=int(row['o'] * 100),
                close_price=int(row['c'] * 100),
                high=int(row['h'] * 100),
                low=int(row['l'] * 100),
                volume=int(row['v']),
                is_hourly=is_hourly
            )
            db.add(stock_history)
        
        db.commit()
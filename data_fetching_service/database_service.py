from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd
import logging
import yfinance as yf
from database import get_db, StockHistory, Stock, Blacklist
from sqlalchemy import select, delete

logger = logging.getLogger(__name__)


class DatabaseService:
    """Handles database operations for stock data and blacklist management."""
    
    def ensure_stock_exists(self, ticker: str, db) -> None:
        """Ensure the stock exists in the stock table. If not, fetch company name and insert."""
        result = db.execute(select(Stock).where(Stock.symbol == ticker)).first()
        
        if result is None:
            try:
                stock_info = yf.Ticker(ticker)
                company_name = stock_info.info.get('longName', stock_info.info.get('shortName', ticker))
                
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
                    existing = db.execute(
                        select(StockHistory).where(
                            StockHistory.stock_symbol == ticker,
                            StockHistory.day_and_time == timestamp,
                            StockHistory.is_hourly == is_hourly
                        )
                    ).first()
                    
                    if existing:
                        logger.debug(f"Skipping duplicate record for {ticker} at {timestamp}")
                        continue
                    
                    open_col = 'Open' if 'Open' in row else 'open'
                    close_col = 'Close' if 'Close' in row else 'close'
                    high_col = 'High' if 'High' in row else 'high'
                    low_col = 'Low' if 'Low' in row else 'low'
                    volume_col = 'Volume' if 'Volume' in row else 'volume'
                    
                    stock_record = StockHistory(
                        stock_symbol=ticker,
                        day_and_time=timestamp,
                        open_price=int(row[open_col] * 100),
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

            if rows_inserted > 0:
                db.commit()
                logger.info(f"✓ SAVED {rows_inserted} rows for {ticker} (is_hourly={is_hourly})")
            else:
                logger.warning(f"No new rows to save for {ticker} (is_hourly={is_hourly}) - all records already exist")
        
        return rows_inserted
    
    def add_to_blacklist(self, ticker: str, gap_start: datetime, is_hourly: bool = True) -> None:
        """Add a gap to the blacklist."""
        with get_db() as db:
            blacklist_entry = Blacklist(
                stock_symbol=ticker,
                timestamp=gap_start,
                time_added=datetime.now(),
                is_hourly=is_hourly
            )
            db.add(blacklist_entry)
            db.commit()
            logger.info(f"Added {ticker} gap at {gap_start} to blacklist")
    
    def get_blacklist(self, ticker: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get blacklist entries, optionally filtered by ticker."""
        with get_db() as db:
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
                    "time_added": row[0].time_added.isoformat(),
                    "is_hourly": row[0].is_hourly
                }
                for row in results
            ]
    
    def clear_blacklist(self, ticker: Optional[str] = None) -> int:
        """Clear blacklist entries, optionally for a specific ticker."""
        with get_db() as db:
            if ticker:
                result = db.execute(
                    delete(Blacklist).where(Blacklist.stock_symbol == ticker.upper())
                )
                count = result.rowcount
            else:
                result = db.execute(delete(Blacklist))
                count = result.rowcount
            
            db.commit()
            logger.info(f"Cleared {count} blacklist entries{f' for {ticker}' if ticker else ''}")
            return count

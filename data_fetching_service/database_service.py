from datetime import datetime
from typing import List, Dict, Any, Optional
import os
import pandas as pd
import logging
from polygon import RESTClient
from database import get_db, StockHistory, Stock, Blacklist
from sqlalchemy import select, delete

logger = logging.getLogger(__name__)


class DatabaseService:
    """Handles database operations for stock data and blacklist management."""
    
    def ensure_stock_exists(self, ticker: str, db) -> None:
        """Ensure the stock exists in the stock table. If not, fetch company name and insert."""
        result = db.execute(select(Stock).where(Stock.symbol == ticker)).first()
        
        if result is None:
            api_key = os.environ.get('POLYGON_API_KEY')
            company_name = ticker

            if api_key:
                try:
                    client = RESTClient(api_key)
                    details = client.get_ticker_details(ticker)
                    company_name = getattr(details, 'name', None) or ticker
                    if isinstance(company_name, str) and len(company_name) > 100:
                        company_name = company_name[:100]
                    logger.info(f"Fetched company name for {ticker} from Polygon: {company_name}")
                except Exception as e:
                    logger.warning(f"Could not fetch company name for {ticker} from Polygon, using ticker as name: {e}")

            new_stock = Stock(
                symbol=ticker,
                company_name=company_name,
                updated_at=datetime.now()
            )
            db.add(new_stock)
            db.commit()
            logger.info(f"Added stock {ticker} ({company_name}) to stock table")

    def update_stock(self, ticker: str, updates: Dict[str, Any], create_if_missing: bool = False) -> int:
        """Update stock metadata for `ticker` using the provided `updates` map.

        - `updates` is a dict mapping column names to values.
        - Only columns present on the `Stock` table will be applied.
        - If `create_if_missing` is True, a missing stock row will be created via `ensure_stock_exists`.

        Returns the number of rows updated (0 or 1).
        """
        if not updates:
            logger.warning(f"No updates provided for {ticker}")
            return 0

        with get_db() as db:
            allowed_columns = set(Stock.__table__.columns.keys())

            payload: Dict[str, Any] = {}
            for col, val in updates.items():
                if col == 'symbol':
                    continue
                if col in allowed_columns:
                    payload[col] = val
                else:
                    logger.debug(f"Ignoring unknown column '{col}' for Stock")

            if not payload:
                logger.warning(f"No valid columns to update for {ticker}")
                return 0

            existing = db.execute(select(Stock).where(Stock.symbol == ticker)).first()
            if not existing:
                if create_if_missing:
                    self.ensure_stock_exists(ticker, db)
                else:
                    logger.warning(f"Stock {ticker} does not exist and create_if_missing is False")
                    return 0

            result = db.execute(Stock.__table__.update().where(Stock.symbol == ticker).values(**payload))
            try:
                rowcount = result.rowcount
            except Exception:
                rowcount = 0

            db.commit()
            logger.info(f"Updated {rowcount} rows for {ticker}: {payload}")
            return rowcount

    def save_stock_data_to_db(self, ticker: str, df: pd.DataFrame, is_hourly: bool = False) -> int:
        """Save stock data DataFrame to database."""
        if df.empty:
            logger.warning(f"No data to save for {ticker} (is_hourly={is_hourly})")
            return 0
        
        rows_inserted = 0
        
        try:
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
        
        except Exception as e:
            logger.error(f"Database error while saving data for {ticker}: {str(e)}")
            raise

    def add_to_blacklist(self, ticker: str, gap_start: datetime, is_hourly: bool = False) -> None:
        """Add a gap start timestamp to the blacklist."""
        with get_db() as db:
            self.ensure_stock_exists(ticker, db)
            
            existing = db.execute(
                select(Blacklist).where(
                    Blacklist.stock_symbol == ticker.upper(),
                    Blacklist.timestamp == gap_start,
                    Blacklist.is_hourly == is_hourly
                )
            ).first()
            
            if existing:
                logger.info(f"Blacklist entry already exists for {ticker} at {gap_start} (hourly={is_hourly})")
                return
            
            blacklist_entry = Blacklist(
                stock_symbol=ticker.upper(),
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

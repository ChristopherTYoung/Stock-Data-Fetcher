from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from sqlalchemy import select, and_, desc, asc
from sqlalchemy.orm import Session
from pydantic import BaseModel
import logging

from database import get_db, Stock, StockHistory

logger = logging.getLogger(__name__)


class StockInfoResponse(BaseModel):
    symbol: str
    company_name: str
    updated_at: datetime
    latest_price: Optional[float] = None
    latest_timestamp: Optional[datetime] = None


class StockHistoryItem(BaseModel):
    timestamp: datetime
    open_price: float
    close_price: float
    high: float
    low: float
    volume: int
    is_hourly: bool


class StockHistoryResponse(BaseModel):
    symbol: str
    start_date: datetime
    end_date: datetime
    timeframe: str
    data: List[StockHistoryItem]


def get_stock_info(symbol: str) -> Optional[StockInfoResponse]:
    with get_db() as db:
        stock_result = db.execute(
            select(Stock).where(Stock.symbol == symbol.upper())
        ).first()
        
        if not stock_result:
            return None
            
        stock = stock_result[0]
        
        latest_result = db.execute(
            select(StockHistory)
            .where(StockHistory.stock_symbol == symbol.upper())
            .order_by(desc(StockHistory.day_and_time))
            .limit(1)
        ).first()
        
        
        latest_price = None
        latest_timestamp = None
        
        if latest_result:
            latest = latest_result[0]
            latest_price = latest.close_price / 100.0
            latest_timestamp = latest.day_and_time
            
        return StockInfoResponse(
            symbol=stock.symbol,
            company_name=stock.company_name,
            updated_at=stock.updated_at,
            latest_price=latest_price,
            latest_timestamp=latest_timestamp,
        )

def get_stock_history(
    symbol: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    ishourly: Optional[bool] = None,
    limit: int = 1000,
    offset: int = 0
) -> Optional[StockHistoryResponse]:

    with get_db() as db:
        stock_exists = db.execute(
            select(Stock).where(Stock.symbol == symbol.upper())
        ).first()
        
        if not stock_exists:
            return None
            
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=30)
            
        conditions = [
            StockHistory.stock_symbol == symbol.upper(),
            StockHistory.day_and_time >= start_date,
            StockHistory.day_and_time <= end_date
        ]
        
        if ishourly is True:
            conditions.append(StockHistory.is_hourly == True)
        elif ishourly is False:
            conditions.append(StockHistory.is_hourly == False)
        
        history_results = db.execute(
            select(StockHistory)
            .where(and_(*conditions))
            .order_by(asc(StockHistory.day_and_time))
            .offset(offset)
            .limit(limit)
        ).fetchall()
        
        
        
        history_data = []
        for result in history_results:
            record = result[0]
            history_data.append(StockHistoryItem(
                timestamp=record.day_and_time,
                open_price=record.open_price / 100.0,
                close_price=record.close_price / 100.0,
                high=record.high / 100.0,
                low=record.low / 100.0,
                volume=record.volume,
                is_hourly=record.is_hourly
            ))
            
        return StockHistoryResponse(
            symbol=symbol.upper(),
            start_date=start_date,
            end_date=end_date,
            timeframe=str(ishourly) if ishourly is not None else "both",
            data=history_data
        )


def get_available_stocks() -> List[Dict[str, Any]]:
    with get_db() as db:
        stocks_result = db.execute(
            select(Stock).order_by(asc(Stock.symbol))
        ).fetchall()
        
        stocks_list = []
        for result in stocks_result:
            stock = result[0]
            
            
            
            stocks_list.append({
                "symbol": stock.symbol,
                "company_name": stock.company_name,
                "updated_at": stock.updated_at,
            })
            
        return stocks_list


def get_stock_date_range(symbol: str) -> Optional[Dict[str, Any]]:
    with get_db() as db:
        earliest_result = db.execute(
            select(StockHistory.day_and_time)
            .where(StockHistory.stock_symbol == symbol.upper())
            .order_by(asc(StockHistory.day_and_time))
            .limit(1)
        ).first()
        
        latest_result = db.execute(
            select(StockHistory.day_and_time)
            .where(StockHistory.stock_symbol == symbol.upper())
            .order_by(desc(StockHistory.day_and_time))
            .limit(1)
        ).first()
        
        if not earliest_result or not latest_result:
            return None
            
        return {
            "symbol": symbol.upper(),
            "earliest_date": earliest_result[0],
            "latest_date": latest_result[0],
            "available_days": (latest_result[0] - earliest_result[0]).days + 1
        }
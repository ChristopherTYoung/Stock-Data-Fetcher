from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging
import asyncio
from database import init_db
from data_fetcher import DataFetcher
from database_service import DatabaseService
import yfinance as yf
from database import get_db, StockHistory, Stock, init_db, close_db_connections, engine
from stock_service import (
    get_stock_info, 
    get_stock_history, 
    get_available_stocks, 
    get_stock_date_range,
    StockInfoResponse,
    StockHistoryResponse
)
from updateendpoint import update_stock_data
from polygon_stock_service import fetch_and_update_symbols

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Data Fetcher", version="1.0.0")
data_fetcher = DataFetcher()
db_service = DatabaseService()

@app.get("/update-stock-history")
async def update_stock_history(ticker: str, is_hourly: bool = False):
    update_stock_data(ticker, is_hourly=is_hourly)
    return {"message": f"Updated {ticker} data ({'hourly' if is_hourly else 'minute-level'})"}

@app.on_event("startup")
async def startup_event():
    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized")
    logger.info("Using yfinance for hourly (2 years) and minute (1 month) data")
    try:
        logger.info("Starting Polygon metadata sync...")
        loop = asyncio.get_running_loop()
        saved = await loop.run_in_executor(None, fetch_and_update_symbols)
        logger.info(f"Polygon sync complete. Saved {saved} stocks.")
    except Exception as e:
        logger.error(f"Error running Polygon sync: {e}")



@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down application...")
    close_db_connections()
    logger.info("Application shutdown complete")

class StockDataRequest(BaseModel):
    tickers: List[str]
    
    class Config:
        json_schema_extra = {
            "example": {
                "tickers": ["AAPL", "GOOGL", "MSFT"]
            }
        }


async def reset_rate_limit_after_delay():
    """Reset rate limit flag after 1 hour."""
    await asyncio.sleep(3600)
    data_fetcher.rate_limited = False
    data_fetcher.rate_limit_reset_time = None
    logger.info("Rate limit has been reset")

@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "Stock Data Fetcher Worker",
        "version": "1.0.0",
        "data_source": "yfinance",
        "data_coverage": "2 years hourly + 1 month minute"
    }

@app.get("/health")
async def health():
    """Health check endpoint for probes."""
    return {"status": "ok"}


@app.get("/db-status")
async def database_status():
    """Get database connection pool status."""
    try:
        pool = engine.pool
        return {
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid(),
            "pool_timeout": engine.pool._timeout,
            "pool_recycle": engine.pool._recycle
        }
    except Exception as e:
        logger.error(f"Error getting database status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database status error: {str(e)}")

@app.get("/rate-limit-status")
async def rate_limit_status():
    """Check if service is rate limited."""
    if data_fetcher.rate_limited and data_fetcher.rate_limit_reset_time:
        seconds_until_reset = max(0, int((data_fetcher.rate_limit_reset_time - datetime.now()).total_seconds()))
        return {
            "rate_limited": True,
            "status": "limited",
            "message": "Service is rate limited",
            "reset_time": data_fetcher.rate_limit_reset_time.isoformat(),
            "seconds_until_reset": seconds_until_reset
        }
    
    return {
        "rate_limited": False,
        "status": "available",
        "message": "Service is available"
    }

@app.get("/blacklist")
async def get_blacklist(ticker: str = None):
    """Get blacklist entries, optionally filtered by ticker."""
    try:
        entries = db_service.get_blacklist(ticker)
        return {
            "count": len(entries),
            "ticker_filter": ticker,
            "entries": entries
        }
    except Exception as e:
        logger.error(f"Error getting blacklist: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting blacklist: {str(e)}")

@app.get("/stocks")
async def list_stocks():
    """Get a list of all available stocks in the database."""
    try:
        stocks = get_available_stocks()
        return {
            "total_stocks": len(stocks),
            "stocks": stocks
        }
    except Exception as e:
        logger.error(f"Error in list_stocks: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/stock/{symbol}", response_model=StockInfoResponse)
async def get_stock(symbol: str):
    """Get comprehensive information about a specific stock."""
    try:
        stock_info = get_stock_info(symbol)
        if not stock_info:
            raise HTTPException(status_code=404, detail=f"Stock {symbol.upper()} not found")
        return stock_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_stock for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/stock/{symbol}/history", response_model=StockHistoryResponse)
async def get_stock_history_endpoint(
    symbol: str,
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    ishourly: Optional[bool] = Query(None, description="True for hourly data, False for minute data, None for both"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip for pagination")
):
    """Get stock price history for a given symbol and date range."""
    try:
        # Parse date strings if provided
        parsed_start_date = None
        parsed_end_date = None
        
        if start_date:
            try:
                parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD")
                
        if end_date:
            try:
                parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD")
                
        # Validate date range
        if parsed_start_date and parsed_end_date and parsed_start_date >= parsed_end_date:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")
        
        history = get_stock_history(
            symbol=symbol,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            ishourly=ishourly,
            limit=limit,
            offset=offset
        )
        
        if not history:
            raise HTTPException(status_code=404, detail=f"Stock {symbol.upper()} not found")
            
        return history
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_stock_history for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/stock/{symbol}/date-range")
async def get_stock_date_range_endpoint(symbol: str):
    """Get the available date range for a specific stock."""
    try:
        date_range = get_stock_date_range(symbol)
        if not date_range:
            raise HTTPException(status_code=404, detail=f"No data found for stock {symbol.upper()}")
        return date_range
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_stock_date_range for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

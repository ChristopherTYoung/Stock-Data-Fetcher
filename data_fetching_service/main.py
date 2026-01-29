from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import logging
import time
import asyncio
import yfinance as yf
from database import get_db, StockHistory, Stock, init_db
from stock_service import (
    get_stock_info, 
    get_stock_history, 
    get_available_stocks, 
    get_stock_date_range,
    StockInfoResponse,
    StockHistoryResponse
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Data Fetcher", version="1.0.0")
rate_limited = False
rate_limit_reset_time = None

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized")
    logger.info("Using yfinance for hourly (2 years) and minute (1 month) data")

class StockDataRequest(BaseModel):
    tickers: List[str]
    
    class Config:
        json_schema_extra = {
            "example": {
                "tickers": ["AAPL", "GOOGL", "MSFT"]
            }
        }


def ensure_stock_exists(ticker: str, db) -> None:
    """Ensure the stock exists in the stock table. If not, fetch company name and insert."""
    from sqlalchemy import select
    
    # Check if stock already exists
    result = db.execute(select(Stock).where(Stock.symbol == ticker)).first()
    
    if result is None:
        # Stock doesn't exist, fetch company info from yfinance
        try:
            stock_info = yf.Ticker(ticker)
            company_name = stock_info.info.get('longName', stock_info.info.get('shortName', ticker))
            
            # Create new stock record
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
            # Fallback: use ticker as company name
            new_stock = Stock(
                symbol=ticker,
                company_name=ticker,
                updated_at=datetime.now()
            )
            db.add(new_stock)
            db.commit()
            logger.info(f"Added stock {ticker} to stock table with ticker as name")


def save_stock_data_to_db(ticker: str, df: pd.DataFrame, is_hourly: bool = False) -> int:
    if df.empty:
        logger.warning(f"No data to save for {ticker} (is_hourly={is_hourly})")
        return 0
    
    rows_inserted = 0
    
    with get_db() as db:
        ensure_stock_exists(ticker, db)
        
        for timestamp, row in df.iterrows():
            try:
                # Handle both uppercase (yfinance) and lowercase column names
                open_col = 'Open' if 'Open' in row else 'open'
                close_col = 'Close' if 'Close' in row else 'close'
                high_col = 'High' if 'High' in row else 'high'
                low_col = 'Low' if 'Low' in row else 'low'
                volume_col = 'Volume' if 'Volume' in row else 'volume'
                
                stock_record = StockHistory(
                    stock_symbol=ticker,
                    day_and_time=timestamp,
                    open_price=int(row[open_col] * 100),  # Convert to cents
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
        
        # Commit in batches for better performance
        if rows_inserted > 0:
            db.commit()
            logger.info(f"✓ SAVED {rows_inserted} rows for {ticker} (is_hourly={is_hourly})")
        else:
            logger.error(f"✗ FAILED to save any data for {ticker} (is_hourly={is_hourly})")
            raise Exception(f"Failed to insert any rows for {ticker}")
    
    return rows_inserted


def fetch_ticker_hourly_data(ticker: str, max_retries: int = 3) -> pd.DataFrame:

    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching hourly data for {ticker} (attempt {attempt + 1}/{max_retries})")
            df = yf.download(
                tickers=[ticker],
                period="2y",
                interval='1h'
            )
            
            if not df.empty:
                logger.info(f"Fetched {len(df)} hourly rows for {ticker}")
                return df
            
            if attempt < max_retries - 1:
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"Error fetching hourly data for {ticker}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(5)
    time.sleep(3)
    
    return pd.DataFrame()


def fetch_ticker_minute_data(ticker: str, start_date: datetime, end_date: datetime, max_retries: int = 3) -> pd.DataFrame:
    all_data = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=6), end_date)
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching minute data for {ticker} from {current_start.date()} to {current_end.date()}")
                df = yf.download(
                    tickers=ticker,
                    start=current_start.strftime('%Y-%m-%d'),
                    end=current_end.strftime('%Y-%m-%d'),
                    interval='1m'
                )
                
                if not df.empty:
                    all_data.append(df)
                    logger.info(f"Fetched {len(df)} minute rows for {ticker}")
                break
                
            except Exception as e:
                logger.error(f"Error fetching minute data for {ticker}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        
        current_start = current_end
        time.sleep(3)  # Delay between chunks
    
    if all_data:
        combined_df = pd.concat(all_data)
        logger.info(f"Total minute data for {ticker}: {len(combined_df)} rows")
        return combined_df
    
    return pd.DataFrame()


def fetch_all_tickers_historical_data(tickers: List[str], end_date: datetime) -> Dict[str, Any]:
    """Fetch 2 years of hourly data + 1 month of minute data for tickers."""
    if rate_limited:
        return {
            "results": {},
            "summary": {
                "total_tickers": len(tickers),
                "successful": 0,
                "failed": len(tickers),
                "failed_tickers": tickers,
                "total_rows_inserted": 0,
                "error": "Service is rate limited"
            }
        }
    
    minute_start_date = end_date - timedelta(days=30)
    
    logger.info(f"Fetching data for {len(tickers)} tickers:")
    logger.info(f"  - Minute: {minute_start_date.date()} to {end_date.date()} (1 month)")
    
    results = {}
    failed_tickers = []
    total_rows_inserted = 0
    
    for idx, ticker in enumerate(tickers, 1):
        try:
            logger.info(f"Processing {ticker} ({idx}/{len(tickers)})")
            ticker_rows = 0
            
            # Fetch hourly data (2 years)
            logger.info(f"  Fetching 2 years of hourly data for {ticker}...")
            hourly_df = fetch_ticker_hourly_data(ticker)
            if not hourly_df.empty:
                rows = save_stock_data_to_db(ticker, hourly_df, is_hourly=True)
                ticker_rows += rows
                logger.info(f"  Saved {rows} hourly rows for {ticker}")
            
            # Fetch minute data (1 month)
            logger.info(f"  Fetching 1 month of minute data for {ticker}...")
            minute_df = fetch_ticker_minute_data(ticker, minute_start_date, end_date)
            if not minute_df.empty:
                rows = save_stock_data_to_db(ticker, minute_df, is_hourly=False)
                ticker_rows += rows
                logger.info(f"  Saved {rows} minute rows for {ticker}")
            
            total_rows_inserted += ticker_rows
            
            if ticker_rows > 0:
                results[ticker] = {
                    "success": True,
                    "rows_inserted": ticker_rows,
                    "hourly_rows": len(hourly_df) if not hourly_df.empty else 0,
                    "minute_rows": len(minute_df) if not minute_df.empty else 0
                }
                logger.info(f"✓✓✓ SUCCESS: {ticker} - {ticker_rows} total rows saved to database")
            else:
                results[ticker] = {
                    "success": False,
                    "error": "No data returned from yfinance"
                }
                failed_tickers.append(ticker)
                logger.error(f"✗✗✗ FAILED: {ticker} - No data fetched from yfinance")
            
            # Delay between tickers to avoid rate limiting
            if idx < len(tickers):
                time.sleep(3)
                
        except Exception as e:
            results[ticker] = {
                "success": False,
                "error": str(e)
            }
            failed_tickers.append(ticker)
            logger.error(f"Failed to process data for {ticker}: {str(e)}")
    
    return {
        "results": results,
        "summary": {
            "total_tickers": len(tickers),
            "successful": len(tickers) - len(failed_tickers),
            "failed": len(failed_tickers),
            "failed_tickers": failed_tickers,
            "total_rows_inserted": total_rows_inserted
        }
    }

async def reset_rate_limit_after_delay():
    """Reset rate limit flag after 1 hour."""
    global rate_limited, rate_limit_reset_time
    await asyncio.sleep(3600)  # Wait 1 hour
    rate_limited = False
    rate_limit_reset_time = None
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

@app.get("/rate-limit-status")
async def rate_limit_status():
    """Check if service is rate limited."""
    global rate_limited, rate_limit_reset_time
    
    if rate_limited and rate_limit_reset_time:
        seconds_until_reset = max(0, int((rate_limit_reset_time - datetime.now()).total_seconds()))
        return {
            "rate_limited": True,
            "status": "limited",
            "message": "Service is rate limited",
            "reset_time": rate_limit_reset_time.isoformat(),
            "seconds_until_reset": seconds_until_reset
        }
    
    return {
        "rate_limited": False,
        "status": "available",
        "message": "Service is available"
    }

@app.post("/fetch-stock-data")
async def fetch_stock_data(request: StockDataRequest):
    global rate_limited, rate_limit_reset_time
    
    if rate_limited:
        raise HTTPException(status_code=429, detail="Service is rate limited. Please try again later.")
    
    if not request.tickers:
        raise HTTPException(status_code=400, detail="No tickers provided")
    
    if len(request.tickers) > 250:
        raise HTTPException(status_code=400, detail="Maximum 250 tickers allowed")
    
    logger.info(f"Starting data fetch for {len(request.tickers)} tickers using yfinance")
    logger.info("Will fetch: 2 years of hourly data + 1 month of minute data per ticker")
    
    end_date = datetime.now()
    
    try:
        results = fetch_all_tickers_historical_data(request.tickers, end_date)
        
        # Check if we hit any errors that suggest rate limiting
        if results['summary']['failed'] > results['summary']['successful'] * 0.5:
            # More than 50% failed - might be rate limited
            rate_limited = True
            rate_limit_reset_time = datetime.now() + timedelta(hours=1)
            asyncio.create_task(reset_rate_limit_after_delay())
            logger.warning("High failure rate detected, enabling rate limit protection")
        
        logger.info(f"Completed: {results['summary']['successful']} successful, "
                   f"{results['summary']['failed']} failed")
        
        return results
    
    except Exception as e:
        logger.error(f"Error in fetch_stock_data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


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

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime, timedelta
import logging
import asyncio
from database import init_db
from data_fetcher import DataFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Data Fetcher", version="1.0.0")
data_fetcher = DataFetcher()

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


async def reset_rate_limit_after_delay():
    """Reset rate limit flag after 1 hour."""
    await asyncio.sleep(3600)  # Wait 1 hour
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
    
@app.get("/check-gaps/{ticker}")
async def check_gaps(ticker: str):
    """Check for data gaps in a stock's historical data."""
    try:
        gaps = data_fetcher.gap_detector.check_for_gaps(ticker.upper())
        
        return {
            "ticker": ticker.upper(),
            "gaps_found": len(gaps),
            "gaps": [
                {
                    "start": gap[0].isoformat(),
                    "end": gap[1].isoformat(),
                    "is_hourly": gap[2],
                    "duration_days": (gap[1] - gap[0]).days
                }
                for gap in gaps
            ]
        }
    except Exception as e:
        logger.error(f"Error checking gaps for {ticker}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error checking gaps: {str(e)}")

@app.post("/fill-gaps/{ticker}")
async def fill_gaps(ticker: str, max_retries: int = 2):
    """Detect and fill gaps in a stock's historical data."""
    if data_fetcher.rate_limited:
        raise HTTPException(status_code=429, detail="Service is rate limited. Please try again later.")
    
    try:
        logger.info(f"Starting gap detection and filling for {ticker} with max_retries={max_retries}")
        result = data_fetcher.detect_and_fill_gaps(ticker, max_retries=max_retries)
        
        # Check if we should enable rate limiting based on failures
        if result.get('gaps_failed', 0) > result.get('gaps_filled', 0):
            data_fetcher.rate_limited = True
            data_fetcher.rate_limit_reset_time = datetime.now() + timedelta(hours=1)
            asyncio.create_task(reset_rate_limit_after_delay())
            logger.warning("High failure rate in gap filling, enabling rate limit protection")
        
        return result
    except Exception as e:
        logger.error(f"Error filling gaps for {ticker}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error filling gaps: {str(e)}")

@app.get("/blacklist")
async def get_blacklist(ticker: str = None):
    """Get blacklist entries, optionally filtered by ticker."""
    try:
        entries = data_fetcher.get_blacklist(ticker)
        return {
            "count": len(entries),
            "ticker_filter": ticker,
            "entries": entries
        }
    except Exception as e:
        logger.error(f"Error getting blacklist: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting blacklist: {str(e)}")

@app.delete("/blacklist")
async def clear_blacklist(ticker: str = None):
    """Clear blacklist entries, optionally for a specific ticker."""
    try:
        count = data_fetcher.clear_blacklist(ticker)
        return {
            "message": f"Cleared {count} blacklist entries{f' for {ticker}' if ticker else ''}",
            "count": count
        }
    except Exception as e:
        logger.error(f"Error clearing blacklist: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error clearing blacklist: {str(e)}")

@app.post("/fetch-stock-data")
async def fetch_stock_data(request: StockDataRequest):
    if data_fetcher.rate_limited:
        raise HTTPException(status_code=429, detail="Service is rate limited. Please try again later.")
    
    if not request.tickers:
        raise HTTPException(status_code=400, detail="No tickers provided")
    
    if len(request.tickers) > 250:
        raise HTTPException(status_code=400, detail="Maximum 250 tickers allowed")
    
    logger.info(f"Starting data fetch for {len(request.tickers)} tickers using yfinance")
    logger.info("Will fetch: 2 years of hourly data + 1 month of minute data per ticker")
    
    end_date = datetime.now()
    
    try:
        results = data_fetcher.fetch_all_tickers_historical_data(request.tickers, end_date)
        
        # Check if we hit any errors that suggest rate limiting
        if results['summary']['failed'] > results['summary']['successful'] * 0.5:
            # More than 50% failed - might be rate limited
            data_fetcher.rate_limited = True
            data_fetcher.rate_limit_reset_time = datetime.now() + timedelta(hours=1)
            asyncio.create_task(reset_rate_limit_after_delay())
            logger.warning("High failure rate detected, enabling rate limit protection")
        
        logger.info(f"Completed: {results['summary']['successful']} successful, "
                   f"{results['summary']['failed']} failed")
        
        return results
    
    except Exception as e:
        logger.error(f"Error in fetch_stock_data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

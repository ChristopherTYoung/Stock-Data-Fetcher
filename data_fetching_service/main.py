from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime, timedelta
import logging
import asyncio
from database import init_db
from data_fetcher import DataFetcher
from database_service import DatabaseService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Data Fetcher", version="1.0.0")
data_fetcher = DataFetcher()
db_service = DatabaseService()

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

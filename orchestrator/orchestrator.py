from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import logging
import threading
import finnhub
import os
from datetime import datetime, timedelta
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Orchestrator", version="1.0.0")

# Global state
stock_queue: List[str] = []
processed_stocks: List[str] = []
queue_lock = threading.Lock()
last_refresh_time: Optional[datetime] = None
finnhub_client: Optional[finnhub.Client] = None

# Configuration
STOCKS_PER_REQUEST = 500
REFRESH_INTERVAL_HOURS = 24  # Refresh stock list every 24 hours


class StockBatchResponse(BaseModel):
    """Response model for stock batch allocation."""
    tickers: List[str]
    batch_size: int
    remaining_in_queue: int
    total_processed: int
    timestamp: str


class OrchestratorStatus(BaseModel):
    """Status of the orchestrator."""
    total_stocks: int
    remaining_in_queue: int
    total_processed: int
    last_refresh: Optional[str]
    next_refresh: Optional[str]


def init_finnhub_client():
    """Initialize Finnhub client with API token."""
    global finnhub_client
    
    token = os.getenv("FINNHUB_TOKEN")
    if not token:
        logger.error("FINNHUB_TOKEN environment variable not set")
        raise ValueError("FINNHUB_TOKEN is required")
    
    finnhub_client = finnhub.Client(api_key=token)
    logger.info("Finnhub client initialized")


def fetch_stock_list_from_finnhub() -> List[str]:
    """Fetch list of all US stocks from Finnhub."""
    if not finnhub_client:
        raise ValueError("Finnhub client not initialized")
    
    try:
        logger.info("Fetching stock list from Finnhub...")
        
        us_stocks = finnhub_client.stock_symbols('US')
        
        tickers = [
            stock['symbol'] 
            for stock in us_stocks 
            if (stock['symbol'] and
                len(stock['symbol']) <= 10 and
                not stock['symbol'].startswith('$') and
                not stock['symbol'].startswith('^'))
        ]
        
        logger.info(f"Fetched {len(tickers)} stocks from US exchanges")
        return tickers
        
    except Exception as e:
        logger.error(f"Error fetching stocks from Finnhub: {str(e)}")
        raise


def refresh_stock_queue():
    """Refresh the stock queue with latest list from Finnhub."""
    global stock_queue, processed_stocks, last_refresh_time
    
    with queue_lock:
        try:
            # Fetch fresh list from Finnhub
            tickers = fetch_stock_list_from_finnhub()
            
            # Reset queues
            stock_queue = tickers.copy()
            processed_stocks = []
            last_refresh_time = datetime.now()
            
            logger.info(f"Stock queue refreshed with {len(stock_queue)} tickers")
            
        except Exception as e:
            logger.error(f"Failed to refresh stock queue: {str(e)}")
            raise


@app.on_event("startup")
async def startup_event():
    """Initialize on startup."""
    logger.info("Starting Stock Orchestrator for one-time bulk download...")
    
    try:
        # Initialize Finnhub client
        init_finnhub_client()
        
        # Load stock list (one-time only, no refresh)
        refresh_stock_queue()
        
        logger.info(f"Stock Orchestrator started successfully with {len(stock_queue)} stocks queued")
        
    except Exception as e:
        logger.error(f"Failed to start orchestrator: {str(e)}")
        raise


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "Stock Orchestrator",
        "version": "1.0.0"
    }


@app.get("/health")
async def health():
    """Health check endpoint for Kubernetes probes."""
    return {"status": "ok"}


@app.get("/status", response_model=OrchestratorStatus)
async def get_status():
    """Get current status of the orchestrator."""
    with queue_lock:
        next_refresh = None
        if last_refresh_time:
            next_refresh = (last_refresh_time + timedelta(hours=REFRESH_INTERVAL_HOURS)).isoformat()
        
        return OrchestratorStatus(
            total_stocks=len(stock_queue) + len(processed_stocks),
            remaining_in_queue=len(stock_queue),
            total_processed=len(processed_stocks),
            last_refresh=last_refresh_time.isoformat() if last_refresh_time else None,
            next_refresh=next_refresh
        )


@app.post("/get-batch", response_model=StockBatchResponse)
async def get_stock_batch(worker_id: Optional[str] = None):
    with queue_lock:
        if not stock_queue:
            logger.info(f"No stocks remaining in queue (Worker: {worker_id})")
            return StockBatchResponse(
                tickers=[],
                batch_size=0,
                remaining_in_queue=0,
                total_processed=len(processed_stocks),
                timestamp=datetime.now().isoformat()
            )
        
        # Get batch of stocks
        batch_size = min(STOCKS_PER_REQUEST, len(stock_queue))
        batch = stock_queue[:batch_size]
        
        # Remove from queue and add to processed
        stock_queue[:batch_size] = []
        processed_stocks.extend(batch)
        
        logger.info(
            f"Allocated {len(batch)} stocks to worker {worker_id or 'unknown'}. "
            f"Remaining: {len(stock_queue)}, Processed: {len(processed_stocks)}"
        )
        
        return StockBatchResponse(
            tickers=batch,
            batch_size=len(batch),
            remaining_in_queue=len(stock_queue),
            total_processed=len(processed_stocks),
            timestamp=datetime.now().isoformat()
        )


@app.post("/refresh")
async def force_refresh():
    """
    Manually trigger a refresh of the stock list.
    
    This will fetch the latest stock list from Finnhub and reset the queue.
    """
    try:
        refresh_stock_queue()
        
        return {
            "success": True,
            "message": "Stock queue refreshed successfully",
            "total_stocks": len(stock_queue),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to refresh: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reset")
async def reset_queue():
    """
    Reset the processed stocks back to the queue.
    
    Useful for re-processing all stocks.
    """
    with queue_lock:
        # Move all processed stocks back to queue
        stock_queue.extend(processed_stocks)
        processed_stocks.clear()
        
        logger.info(f"Queue reset. Total stocks in queue: {len(stock_queue)}")
        
        return {
            "success": True,
            "message": "Queue reset successfully",
            "total_in_queue": len(stock_queue),
            "timestamp": datetime.now().isoformat()
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

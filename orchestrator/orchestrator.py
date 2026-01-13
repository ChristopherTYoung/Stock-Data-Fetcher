from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import logging
import threading
from polygon import RESTClient
import os
from datetime import datetime, timedelta
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Orchestrator", version="1.0.0")

history_queue: List[str] = []
history_processed: List[str] = []
history_lock = threading.Lock()

gap_detection_queue: List[str] = []
gap_detection_processed: List[str] = []
gap_detection_lock = threading.Lock()

last_refresh_time: Optional[datetime] = None
polygon_client: Optional[RESTClient] = None
scheduler = AsyncIOScheduler()

STOCKS_PER_REQUEST = 1000
REFRESH_INTERVAL_HOURS = 24


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
    history_updates: Dict[str, int]
    gap_detection: Dict[str, int]
    last_refresh: Optional[str]
    next_refresh: Optional[str]


def init_polygon_client():
    """Initialize Polygon client with API key."""
    global polygon_client
    
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        logger.error("POLYGON_API_KEY environment variable not set")
        raise ValueError("POLYGON_API_KEY is required")
    
    polygon_client = RESTClient(api_key=api_key)
    logger.info("Polygon client initialized")


def fetch_stock_list_from_polygon() -> List[str]:
    """Fetch list of all US stocks from Polygon."""
    if not polygon_client:
        raise ValueError("Polygon client not initialized")
    
    try:
        logger.info("Fetching stock list from Polygon...")
        
        tickers = []
        next_url = None
        page_count = 0
        
        while True:
            if next_url:
                response = polygon_client.list_tickers(next_url=next_url)
            else:
                response = polygon_client.list_tickers(
                    market='stocks',
                    active=True,
                    limit=1000
                )

            for ticker in response:
                if hasattr(ticker, 'locale') and ticker.locale == 'us':
                    symbol = ticker.ticker

                    if (symbol and
                        not symbol.startswith('$') and
                        not symbol.startswith('^')):
                        tickers.append(symbol)
            
            page_count += 1
            logger.info(f"Page {page_count}: Fetched {len(tickers)} tickers so far...")

            if hasattr(response, 'next_url') and response.next_url:
                next_url = response.next_url
            else:
                break
        
        logger.info(f"Fetched {len(tickers)} US stocks from Polygon across {page_count} pages")
        return tickers
        
    except Exception as e:
        logger.error(f"Error fetching stocks from Polygon: {str(e)}")
        raise


def refresh_stock_queue():
    """Refresh both stock queues with latest list from Polygon."""
    global history_queue, history_processed, gap_detection_queue, gap_detection_processed, last_refresh_time
    
    try:
        tickers = fetch_stock_list_from_polygon()

        with history_lock:
            history_queue = tickers.copy()
            history_processed = []

        with gap_detection_lock:
            gap_detection_queue = tickers.copy()
            gap_detection_processed = []
        
        last_refresh_time = datetime.now()
        
        logger.info(f"Both queues refreshed with {len(tickers)} tickers")
        
    except Exception as e:
        logger.error(f"Failed to refresh stock queues: {str(e)}")
        raise


@app.on_event("startup")
async def startup_event():
    """Initialize on startup."""
    logger.info("Starting Stock Orchestrator with scheduled daily refresh...")
    
    try:
        init_polygon_client()
        
        refresh_stock_queue()
        
        scheduler.add_job(
            refresh_stock_queue,
            trigger=CronTrigger(hour=0, minute=0),
            id='daily_stock_refresh',
            name='Daily Stock List Refresh',
            replace_existing=True
        )

        scheduler.start()
        
        logger.info(f"Stock Orchestrator started successfully")
        logger.info(f"History queue: {len(history_queue)} stocks | Gap detection queue: {len(gap_detection_queue)} stocks")
        logger.info("Scheduled daily refresh at 12:00 AM UTC")
        
    except Exception as e:
        logger.error(f"Failed to start orchestrator: {str(e)}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    scheduler.shutdown()
    logger.info("Scheduler shut down")


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
    with history_lock:
        history_remaining = len(history_queue)
        history_total_processed = len(history_processed)
    
    with gap_detection_lock:
        gap_remaining = len(gap_detection_queue)
        gap_total_processed = len(gap_detection_processed)
    
    next_refresh = None
    if last_refresh_time:
        next_refresh = (last_refresh_time + timedelta(hours=REFRESH_INTERVAL_HOURS)).isoformat()
    
    total_stocks = history_remaining + history_total_processed
    
    return OrchestratorStatus(
        total_stocks=total_stocks,
        history_updates={
            "remaining": history_remaining,
            "processed": history_total_processed
        },
        gap_detection={
            "remaining": gap_remaining,
            "processed": gap_total_processed
        },
        last_refresh=last_refresh_time.isoformat() if last_refresh_time else None,
        next_refresh=next_refresh
    )


@app.post("/get-batch", response_model=StockBatchResponse)
async def get_stock_batch(worker_id: Optional[str] = None):
    """Get batch of stocks for history updates."""
    with history_lock:
        if not history_queue:
            logger.info(f"No stocks remaining in history queue (Worker: {worker_id})")
            return StockBatchResponse(
                tickers=[],
                batch_size=0,
                remaining_in_queue=0,
                total_processed=len(history_processed),
                timestamp=datetime.now().isoformat()
            )

        batch_size = min(STOCKS_PER_REQUEST, len(history_queue))
        batch = history_queue[:batch_size]
        
        history_queue[:batch_size] = []
        history_processed.extend(batch)
        
        logger.info(
            f"[HISTORY] Allocated {len(batch)} stocks to worker {worker_id or 'unknown'}. "
            f"Remaining: {len(history_queue)}, Processed: {len(history_processed)}"
        )
        
        return StockBatchResponse(
            tickers=batch,
            batch_size=len(batch),
            remaining_in_queue=len(history_queue),
            total_processed=len(history_processed),
            timestamp=datetime.now().isoformat()
        )


@app.post("/get-gap-detection-batch", response_model=StockBatchResponse)
async def get_gap_detection_batch(worker_id: Optional[str] = None):
    """Get batch of stocks for gap detection."""
    with gap_detection_lock:
        if not gap_detection_queue:
            logger.info(f"No stocks remaining in gap detection queue (Worker: {worker_id})")
            return StockBatchResponse(
                tickers=[],
                batch_size=0,
                remaining_in_queue=0,
                total_processed=len(gap_detection_processed),
                timestamp=datetime.now().isoformat()
            )

        batch_size = min(STOCKS_PER_REQUEST, len(gap_detection_queue))
        batch = gap_detection_queue[:batch_size]
        
        gap_detection_queue[:batch_size] = []
        gap_detection_processed.extend(batch)
        
        logger.info(
            f"[GAP DETECTION] Allocated {len(batch)} stocks to worker {worker_id or 'unknown'}. "
            f"Remaining: {len(gap_detection_queue)}, Processed: {len(gap_detection_processed)}"
        )
        
        return StockBatchResponse(
            tickers=batch,
            batch_size=len(batch),
            remaining_in_queue=len(gap_detection_queue),
            total_processed=len(gap_detection_processed),
            timestamp=datetime.now().isoformat()
        )


@app.post("/refresh")
async def force_refresh():
    """
    Manually trigger a refresh of the stock list.
    
    This will fetch the latest stock list from Polygon and reset the queue.
    """
    try:
        refresh_stock_queue()
        
        return {
            "success": True,
            "message": "Both queues refreshed successfully",
            "history_queue": len(history_queue),
            "gap_detection_queue": len(gap_detection_queue),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to refresh: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reset")
async def reset_queue():
    """
    Reset the processed stocks back to both queues.
    
    Useful for re-processing all stocks.
    """
    with history_lock:
        history_queue.extend(history_processed)
        history_processed.clear()
    
    with gap_detection_lock:
        gap_detection_queue.extend(gap_detection_processed)
        gap_detection_processed.clear()
    
    logger.info(f"Queues reset. History: {len(history_queue)}, Gap detection: {len(gap_detection_queue)}")
    
    return {
        "success": True,
        "message": "Both queues reset successfully",
        "history_queue": len(history_queue),
        "gap_detection_queue": len(gap_detection_queue),
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

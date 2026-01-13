from fastapi import FastAPI, HTTPException
from typing import Optional
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from models import StockBatchResponse, OrchestratorStatus
from polygon_service import PolygonService
from stock_queue_service import StockQueueService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Orchestrator", version="1.0.0")

# Initialize services
polygon_service = PolygonService()
queue_service = StockQueueService(stocks_per_request=1000, refresh_interval_hours=24)
scheduler = AsyncIOScheduler()


def refresh_stock_queues():
    """Refresh both stock queues with latest list from Polygon."""
    try:
        tickers = polygon_service.fetch_stock_list()
        queue_service.refresh_queues(tickers)
        
    except Exception as e:
        logger.error(f"Failed to refresh stock queues: {str(e)}")
        raise


@app.on_event("startup")
async def startup_event():
    """Initialize on startup."""
    logger.info("Starting Stock Orchestrator with scheduled daily refresh...")
    
    try:
        polygon_service.initialize()
        
        refresh_stock_queues()
        
        scheduler.add_job(
            refresh_stock_queues,
            trigger=CronTrigger(hour=0, minute=0),
            id='daily_stock_refresh',
            name='Daily Stock List Refresh',
            replace_existing=True
        )

        scheduler.start()
        
        status = queue_service.get_status()
        logger.info(f"Stock Orchestrator started successfully")
        logger.info(f"History queue: {status.history_updates['remaining']} stocks | "
                   f"Gap detection queue: {status.gap_detection['remaining']} stocks")
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
    return queue_service.get_status()


@app.post("/get-batch", response_model=StockBatchResponse)
async def get_stock_batch(worker_id: Optional[str] = None):
    """Get batch of stocks for history updates."""
    return queue_service.get_batch(worker_id)


@app.post("/get-gap-detection-batch", response_model=StockBatchResponse)
async def get_gap_detection_batch(worker_id: Optional[str] = None):
    """Get batch of stocks for gap detection."""
    return queue_service.get_gap_detection_batch(worker_id)


@app.post("/refresh")
async def force_refresh():
    """
    Manually trigger a refresh of the stock list.
    
    This will fetch the latest stock list from Polygon and reset the queue.
    """
    try:
        refresh_stock_queues()
        status = queue_service.get_status()
        
        return {
            "success": True,
            "message": "Both queues refreshed successfully",
            "history_queue": status.history_updates['remaining'],
            "gap_detection_queue": status.gap_detection['remaining'],
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
    return queue_service.reset_queues()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

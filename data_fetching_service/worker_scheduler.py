"""
Scheduled worker that fetches stock batches from orchestrator every hour.
"""
import asyncio
import httpx
import logging
from datetime import datetime
import os

# Import the data fetching function from main.py
from main import fetch_all_tickers_historical_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8080")
WORKER_ID = os.getenv("WORKER_ID", "worker-1")
FETCH_INTERVAL_SECONDS = 3600  # 1 hour


async def fetch_batch_from_orchestrator() -> list:
    """Fetch a batch of 500 stocks from the orchestrator."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{ORCHESTRATOR_URL}/get-batch",
                params={"worker_id": WORKER_ID}
            )
            response.raise_for_status()
            data = response.json()
            
            tickers = data.get("tickers", [])
            logger.info(f"Received {len(tickers)} tickers from orchestrator")
            logger.info(f"Remaining in queue: {data.get('remaining_in_queue', 0)}")
            
            return tickers
            
    except Exception as e:
        logger.error(f"Error fetching batch from orchestrator: {str(e)}")
        return []


async def process_stock_batch(tickers: list):
    """Process stock batch using main.py's fetch function."""
    if not tickers:
        logger.info("No tickers to process")
        return
    
    try:
        logger.info(f"Processing {len(tickers)} tickers...")
        
        # Use the actual data fetching function from main.py
        # This fetches 2 years of hourly data + 1 month of minute data
        end_date = datetime.now()
        results = fetch_all_tickers_historical_data(tickers, end_date)
        
        summary = results.get("summary", {})
        logger.info(
            f"Batch processing complete: "
            f"{summary.get('successful', 0)} successful, "
            f"{summary.get('failed', 0)} failed, "
            f"{summary.get('total_rows_inserted', 0)} rows inserted"
        )
        
        # Log any failed tickers
        if summary.get('failed_tickers'):
            logger.warning(f"Failed tickers: {summary.get('failed_tickers')}")
            
    except Exception as e:
        logger.error(f"Error processing stock batch: {str(e)}")
        import traceback
        traceback.print_exc()


async def hourly_fetch_task():
    """Background task that fetches and processes stocks every hour."""
    logger.info(f"Starting hourly fetch task (Worker ID: {WORKER_ID})")
    logger.info(f"Orchestrator URL: {ORCHESTRATOR_URL}")
    logger.info(f"Fetch interval: {FETCH_INTERVAL_SECONDS} seconds")
    
    while True:
        try:
            logger.info(f"[{datetime.now()}] Starting hourly fetch cycle...")
            
            # Fetch batch from orchestrator
            tickers = await fetch_batch_from_orchestrator()
            
            # Process the batch
            if tickers:
                await process_stock_batch(tickers)
            else:
                logger.info("No stocks available to process")
            
            logger.info(f"Fetch cycle complete. Sleeping for {FETCH_INTERVAL_SECONDS} seconds...")
            
        except Exception as e:
            logger.error(f"Error in hourly fetch cycle: {str(e)}")
        
        # Wait for next cycle
        await asyncio.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(hourly_fetch_task())

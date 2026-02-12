"""
Scheduled worker that fetches stock batches from orchestrator every hour.
"""
import asyncio
import httpx
import logging
from datetime import datetime
import os
import signal
import sys

# Import the DataFetcher class and database cleanup
from data_fetcher import DataFetcher
from database import close_db_connections

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8080")
WORKER_ID = os.getenv("WORKER_ID", "worker-1")
FETCH_INTERVAL_SECONDS = 3600  # 1 hour

# Create a DataFetcher instance
data_fetcher = DataFetcher()


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
            logger.info(f"[HISTORY] Received {len(tickers)} tickers from orchestrator")
            logger.info(f"[HISTORY] Remaining in queue: {data.get('remaining_in_queue', 0)}")
            
            return tickers
            
    except Exception as e:
        logger.error(f"Error fetching batch from orchestrator: {str(e)}")
        return []


async def fetch_gap_detection_batch() -> list:
    """Fetch a batch of stocks for gap detection from the orchestrator."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{ORCHESTRATOR_URL}/get-gap-detection-batch",
                params={"worker_id": WORKER_ID}
            )
            response.raise_for_status()
            data = response.json()
            
            tickers = data.get("tickers", [])
            logger.info(f"[GAP DETECTION] Received {len(tickers)} tickers from orchestrator")
            logger.info(f"[GAP DETECTION] Remaining in queue: {data.get('remaining_in_queue', 0)}")
            
            return tickers
            
    except Exception as e:
        logger.error(f"Error fetching gap detection batch from orchestrator: {str(e)}")
        return []


async def process_stock_batch(tickers: list):
    """Process stock batch using DataFetcher class."""
    if not tickers:
        logger.info("No tickers to process")
        return
    
    try:
        logger.info(f"[HISTORY] Processing {len(tickers)} tickers...")
        
        # Use the DataFetcher instance to fetch data
        # This fetches 2 years of hourly data + 1 month of minute data
        end_date = datetime.now()
        results = data_fetcher.fetch_all_tickers_historical_data(tickers, end_date)
        
        summary = results.get("summary", {})
        logger.info(
            f"[HISTORY] Batch processing complete: "
            f"{summary.get('successful', 0)} successful, "
            f"{summary.get('failed', 0)} failed, "
            f"{summary.get('total_rows_inserted', 0)} rows inserted"
        )
        
        # Log any failed tickers
        if summary.get('failed_tickers'):
            logger.warning(f"[HISTORY] Failed tickers: {summary.get('failed_tickers')}")
            
    except Exception as e:
        logger.error(f"Error processing stock batch: {str(e)}")
        import traceback
        traceback.print_exc()


async def process_gap_detection_batch(tickers: list):
    """Process gap detection for a batch of tickers."""
    if not tickers:
        logger.info("No tickers to process for gap detection")
        return
    
    try:
        logger.info(f"[GAP DETECTION] Processing {len(tickers)} tickers...")
        
        total_gaps_found = 0
        total_gaps_filled = 0
        
        for ticker in tickers:
            # Use DataFetcher's detect_and_fill_gaps method
            result = data_fetcher.detect_and_fill_gaps(ticker)
            
            gaps_found = result.get('gaps_found', 0)
            gaps_filled = result.get('gaps_filled', 0)
            
            if gaps_found > 0:
                logger.info(
                    f"[GAP DETECTION] {ticker}: {gaps_found} gaps found, "
                    f"Attempted to fill {gaps_filled} gaps"
                )
                total_gaps_found += gaps_found
                total_gaps_filled += gaps_filled
        
        logger.info(
            f"[GAP DETECTION] Batch processing complete: "
            f"{total_gaps_found} gaps found, {total_gaps_filled} gaps filled"
        )
            
    except Exception as e:
        logger.error(f"Error processing gap detection batch: {str(e)}")
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
            
            # Priority 1: Fetch batch for history updates from orchestrator
            history_tickers = await fetch_batch_from_orchestrator()
            
            # Process the history batch if available
            if history_tickers:
                await process_stock_batch(history_tickers)
            else:
                logger.info("[HISTORY] No history update work available")
                
                # Priority 2: Only check gap detection if no history work
                gap_tickers = await fetch_gap_detection_batch()
                
                if gap_tickers:
                    await process_gap_detection_batch(gap_tickers)
                else:
                    logger.info("[GAP DETECTION] No gap detection work available")
                    logger.info("No work available in either queue")
            
            logger.info(f"Fetch cycle complete. Sleeping for {FETCH_INTERVAL_SECONDS} seconds...")
            
        except Exception as e:
            logger.error(f"Error in hourly fetch cycle: {str(e)}")
        
        # Wait for next cycle
        await asyncio.sleep(FETCH_INTERVAL_SECONDS)


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    close_db_connections()
    sys.exit(0)


if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        dev = os.getenv("DEV")
        if not dev:
            asyncio.run(hourly_fetch_task())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        close_db_connections()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        close_db_connections()
        raise

"""Scheduled worker that pulls stock calculation batches from the orchestrator."""

import asyncio
import logging
import os
import signal
import sys
import threading

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from stock_data_calculator.database import close_db_connections
from stock_data_calculator.logging_config import setup_logging
from stock_data_calculator.polygon_stock_service import (
    update_metadata_for_tickers,
    update_quarterly_metrics_for_tickers,
)

logger = setup_logging("stock-data-calculator", level=logging.INFO)

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8080")
WORKER_ID = os.getenv("WORKER_ID", "stock-calculator-worker-1")
scheduler = AsyncIOScheduler()


async def fetch_calculation_batch_from_orchestrator() -> list[str]:
    """Fetch a regular stock-calculation batch from orchestrator."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{ORCHESTRATOR_URL}/get-stock-calculation-batch",
                params={"worker_id": WORKER_ID},
            )
            response.raise_for_status()
            data = response.json()

            tickers = data.get("tickers", [])
            logger.info("[STOCK CALCULATION] Received %s tickers from orchestrator", len(tickers))
            logger.info(
                "[STOCK CALCULATION] Remaining in queue: %s",
                data.get("remaining_in_queue", 0),
            )

            return tickers

    except Exception as e:
        logger.error("Error fetching stock calculation batch from orchestrator: %s", str(e))
        return []


async def fetch_quarterly_update_batch_from_orchestrator() -> list[str]:
    """Fetch a quarterly update batch from orchestrator."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{ORCHESTRATOR_URL}/get-quarterly-update-batch",
                params={"worker_id": WORKER_ID},
            )
            response.raise_for_status()
            data = response.json()

            tickers = data.get("tickers", [])
            logger.info("[QUARTERLY UPDATE] Received %s tickers from orchestrator", len(tickers))
            logger.info(
                "[QUARTERLY UPDATE] Remaining in queue: %s",
                data.get("remaining_in_queue", 0),
            )

            return tickers

    except Exception as e:
        logger.error("Error fetching quarterly update batch from orchestrator: %s", str(e))
        return []


async def process_stock_calculation_batch(tickers: list[str]) -> None:
    """Run regular calculation updates for the assigned stock list on a thread."""
    if not tickers:
        logger.info("[STOCK CALCULATION] No tickers to process")
        return

    try:
        logger.info("[STOCK CALCULATION] Starting thread to process %s tickers...", len(tickers))
        
        # Start calculation on a separate thread
        calculation_thread = threading.Thread(
            target=update_metadata_for_tickers,
            args=(tickers, None),
            name="stock-calculation-worker-thread",
            daemon=False
        )
        calculation_thread.start()
        logger.info("[STOCK CALCULATION] Thread started with ID: %s", calculation_thread.ident)
        
        # Wait for thread to complete
        calculation_thread.join()
        logger.info("[STOCK CALCULATION] Thread completed")
        
    except Exception as e:
        logger.error("Error processing stock calculation batch: %s", str(e))


async def process_quarterly_update_batch(tickers: list[str]) -> None:
    """Run quarterly financial calculations for the assigned stock list on a thread."""
    if not tickers:
        logger.info("[QUARTERLY UPDATE] No tickers to process")
        return

    try:
        logger.info("[QUARTERLY UPDATE] Starting thread to process %s tickers...", len(tickers))
        
        # Start quarterly update on a separate thread
        quarterly_thread = threading.Thread(
            target=update_quarterly_metrics_for_tickers,
            args=(tickers, None),
            name="quarterly-update-worker-thread",
            daemon=False
        )
        quarterly_thread.start()
        logger.info("[QUARTERLY UPDATE] Thread started with ID: %s", quarterly_thread.ident)
        
        # Wait for thread to complete
        quarterly_thread.join()
        logger.info("[QUARTERLY UPDATE] Thread completed")
        
    except Exception as e:
        logger.error("Error processing quarterly update batch: %s", str(e))




async def run_calculation_cycle() -> None:
    """Run one stock calculation pull/process cycle (regular updates only)."""
    try:
        logger.info("[STOCK CALCULATION] Starting scheduled calculation cycle")

        tickers = await fetch_calculation_batch_from_orchestrator()
        if tickers:
            await process_stock_calculation_batch(tickers)
        else:
            logger.info("[STOCK CALCULATION] No work available")

        logger.info("[STOCK CALCULATION] Cycle complete")
    except Exception as e:
        logger.error("Error in scheduled stock calculation cycle: %s", str(e))


async def run_quarterly_update_cycle() -> None:
    """Run quarterly update cycle (runs at midnight and on startup)."""
    try:
        logger.info("[QUARTERLY UPDATE] Starting quarterly update cycle")

        quarterly_tickers = await fetch_quarterly_update_batch_from_orchestrator()
        if quarterly_tickers:
            await process_quarterly_update_batch(quarterly_tickers)
        else:
            logger.info("[QUARTERLY UPDATE] No work available")

        logger.info("[QUARTERLY UPDATE] Cycle complete")
    except Exception as e:
        logger.error("Error in quarterly update cycle: %s", str(e))


def schedule_calculation_task() -> None:
    """Schedule calculation cycle every 10 minutes and quarterly updates at midnight + startup."""
    # Regular calculation cycle every 10 minutes
    scheduler.add_job(
        run_calculation_cycle,
        trigger=CronTrigger(minute="*/10"),
        id="ten_minute_stock_calculation_worker_fetch",
        name="Ten-Minute Stock Calculation Worker Fetch",
        replace_existing=True,
    )
    
    # Quarterly update cycle at midnight (00:00 UTC)
    scheduler.add_job(
        run_quarterly_update_cycle,
        trigger=CronTrigger(hour=0, minute=0),
        id="midnight_quarterly_update_cycle",
        name="Midnight Quarterly Update Cycle",
        replace_existing=True,
    )
    
    scheduler.start()


async def run_startup_quarterly_update() -> None:
    """Run quarterly update on startup."""
    try:
        logger.info("[STARTUP] Running quarterly update on startup...")
        await run_quarterly_update_cycle()
    except Exception as e:
        logger.error("Error in startup quarterly update: %s", str(e))



def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Received signal %s. Shutting down gracefully...", signum)
    if scheduler.running:
        scheduler.shutdown()
    close_db_connections()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        dev = os.getenv("DEV")
        if not dev:
            logger.info("Starting stock calculation worker scheduler (Worker ID: %s)", WORKER_ID)
            logger.info("Orchestrator URL: %s", ORCHESTRATOR_URL)
            logger.info("Regular calculation cycle: every 10 minutes (UTC)")
            logger.info("Quarterly update cycle: at midnight (00:00 UTC) and on startup")
            
            schedule_calculation_task()
            
            # Run startup quarterly update
            loop = asyncio.get_event_loop()
            loop.create_task(run_startup_quarterly_update())
            loop.run_forever()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        if scheduler.running:
            scheduler.shutdown()
        close_db_connections()
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        if scheduler.running:
            scheduler.shutdown()
        close_db_connections()
        raise

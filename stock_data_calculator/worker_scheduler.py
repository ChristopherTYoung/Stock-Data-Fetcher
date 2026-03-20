"""Scheduled worker that pulls stock calculation batches from the orchestrator."""

import asyncio
import logging
import os
import signal
import sys

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from stock_data_calculator.database import close_db_connections
from stock_data_calculator.logging_config import setup_logging
from stock_data_calculator.polygon_stock_service import update_metadata_for_tickers

logger = setup_logging("stock-data-calculator", level=logging.INFO)

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8080")
WORKER_ID = os.getenv("WORKER_ID", "stock-calculator-worker-1")
scheduler = AsyncIOScheduler()


async def fetch_calculation_batch_from_orchestrator() -> list[str]:
    """Fetch a stock calculation batch from orchestrator."""
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


async def process_stock_calculation_batch(tickers: list[str]) -> None:
    """Run calculation updates for the assigned stock list."""
    if not tickers:
        logger.info("No tickers to process for stock calculation")
        return

    try:
        logger.info("[STOCK CALCULATION] Processing %s tickers...", len(tickers))
        loop = asyncio.get_running_loop()
        saved = await loop.run_in_executor(None, update_metadata_for_tickers, tickers)
        logger.info("[STOCK CALCULATION] Batch processing complete. Saved %s stocks", saved)
    except Exception as e:
        logger.error("Error processing stock calculation batch: %s", str(e))


async def run_calculation_cycle() -> None:
    """Run one stock calculation pull/process cycle."""
    try:
        logger.info("Starting scheduled stock calculation cycle")
        tickers = await fetch_calculation_batch_from_orchestrator()
        if tickers:
            await process_stock_calculation_batch(tickers)
        else:
            logger.info("[STOCK CALCULATION] No work available")
        logger.info("Stock calculation cycle complete")
    except Exception as e:
        logger.error("Error in scheduled stock calculation cycle: %s", str(e))


def schedule_calculation_task() -> None:
    """Schedule calculation cycle every 10 minutes (UTC)."""
    scheduler.add_job(
        run_calculation_cycle,
        trigger=CronTrigger(minute="*/10"),
        id="ten_minute_stock_calculation_worker_fetch",
        name="Ten-Minute Stock Calculation Worker Fetch",
        replace_existing=True,
    )
    scheduler.start()


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
            logger.info("Scheduled fetch every 10 minutes (UTC)")
            schedule_calculation_task()
            asyncio.get_event_loop().run_forever()
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

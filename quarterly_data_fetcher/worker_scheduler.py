"""Scheduled worker that pulls quarterly update batches from the orchestrator."""

import asyncio
import logging
import os
import signal
import sys
import threading

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from quarterly_data_fetcher.database import close_db_connections
from quarterly_data_fetcher.logging_config import setup_logging
from quarterly_data_fetcher.quarterly_stock_service import update_quarterly_metrics_for_tickers

logger = setup_logging("quarterly-data-fetcher", level=logging.INFO)

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8080")
WORKER_ID = os.getenv("WORKER_ID", "quarterly-data-fetcher-1")
scheduler = AsyncIOScheduler()


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
            logger.info("[QUARTERLY UPDATE] Remaining in queue: %s", data.get("remaining_in_queue", 0))
            return tickers

    except Exception as error:
        logger.error("Error fetching quarterly update batch from orchestrator: %s", error)
        return []


async def process_quarterly_update_batch(tickers: list[str]) -> None:
    """Run quarterly financial calculations for the assigned stock list on a thread."""
    if not tickers:
        logger.info("[QUARTERLY UPDATE] No tickers to process")
        return

    try:
        logger.info("[QUARTERLY UPDATE] Starting thread to process %s tickers...", len(tickers))

        quarterly_thread = threading.Thread(
            target=update_quarterly_metrics_for_tickers,
            args=(tickers, None),
            name="quarterly-update-worker-thread",
            daemon=False,
        )
        quarterly_thread.start()
        logger.info("[QUARTERLY UPDATE] Thread started with ID: %s", quarterly_thread.ident)
        quarterly_thread.join()
        logger.info("[QUARTERLY UPDATE] Thread completed")

    except Exception as error:
        logger.error("Error processing quarterly update batch: %s", error)


async def run_quarterly_update_cycle() -> None:
    """Run one quarterly update pull/process cycle."""
    try:
        logger.info("[QUARTERLY UPDATE] Starting scheduled quarterly update cycle")

        quarterly_tickers = await fetch_quarterly_update_batch_from_orchestrator()
        if quarterly_tickers:
            await process_quarterly_update_batch(quarterly_tickers)
        else:
            logger.info("[QUARTERLY UPDATE] No work available")

        logger.info("[QUARTERLY UPDATE] Cycle complete")
    except Exception as error:
        logger.error("Error in scheduled quarterly update cycle: %s", error)


def schedule_quarterly_task() -> None:
    """Schedule quarterly update cycle at 2:00 AM UTC."""
    scheduler.add_job(
        run_quarterly_update_cycle,
        trigger=CronTrigger(hour=2, minute=0),
        id="two_am_quarterly_update_cycle",
        name="2AM Quarterly Update Cycle",
        replace_existing=True,
    )
    scheduler.start()


def signal_handler(signum, _frame):
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
            logger.info("Starting quarterly data fetcher scheduler (Worker ID: %s)", WORKER_ID)
            logger.info("Orchestrator URL: %s", ORCHESTRATOR_URL)
            logger.info("Quarterly update cycle: daily at 02:00 UTC")

            schedule_quarterly_task()

            loop = asyncio.get_event_loop()
            loop.run_forever()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        if scheduler.running:
            scheduler.shutdown()
        close_db_connections()
    except Exception as error:
        logger.error("Unexpected error: %s", error)
        if scheduler.running:
            scheduler.shutdown()
        close_db_connections()
        raise

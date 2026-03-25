"""Scheduled worker that pulls quarterly update batches from the orchestrator."""

import asyncio
import logging
import os
import signal
import sys
from typing import Optional

import httpx

from quarterly_data_fetcher.database import close_db_connections
from quarterly_data_fetcher.logging_config import setup_logging
from quarterly_data_fetcher.quarterly_stock_service import update_quarterly_metrics_for_tickers

logger = setup_logging("quarterly-data-fetcher", level=logging.INFO)

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8080")
WORKER_ID = os.getenv("WORKER_ID", "quarterly-data-fetcher-1")
_worker_task: Optional[asyncio.Task] = None


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
    """Run quarterly financial calculations for the assigned stock list in-process."""
    if not tickers:
        logger.info("[QUARTERLY UPDATE] No tickers to process")
        return

    try:
        logger.info(
            "[QUARTERLY UPDATE] Processing %s tickers in single-threaded mode (background thread for API responsiveness)...",
            len(tickers),
        )
        # Keep ticker processing single-threaded while preventing the FastAPI event loop from blocking.
        await asyncio.to_thread(update_quarterly_metrics_for_tickers, tickers, None)
        logger.info("[QUARTERLY UPDATE] Processing completed")

    except Exception as error:
        logger.error("Error processing quarterly update batch: %s", error)


async def run_quarterly_update_cycle() -> bool:
    """Run one quarterly update pull/process cycle and return whether work was processed."""
    try:
        logger.info("[QUARTERLY UPDATE] Starting quarterly update cycle")

        quarterly_tickers = await fetch_quarterly_update_batch_from_orchestrator()
        if quarterly_tickers:
            await process_quarterly_update_batch(quarterly_tickers)
            logger.info("[QUARTERLY UPDATE] Cycle complete")
            return True
        else:
            logger.info("[QUARTERLY UPDATE] No work available")
            return False
    except Exception as error:
        logger.error("Error in quarterly update cycle: %s", error)
        return False


async def run_continuous_quarterly_worker() -> None:
    """Continuously fetch and process quarterly batches from orchestrator."""
    no_work_sleep_seconds = float(os.getenv("QUARTERLY_NO_WORK_SLEEP_SECONDS", "60"))
    error_sleep_seconds = float(os.getenv("QUARTERLY_ERROR_SLEEP_SECONDS", "30"))
    startup_delay_seconds = max(float(os.getenv("QUARTERLY_START_DELAY_SECONDS", "0")), 0.0)

    logger.info(
        "[QUARTERLY UPDATE] Continuous worker started (startup delay=%ss, no-work sleep=%ss, error sleep=%ss)",
        startup_delay_seconds,
        no_work_sleep_seconds,
        error_sleep_seconds,
    )

    if startup_delay_seconds > 0:
        logger.info("[QUARTERLY UPDATE] Waiting %.0fs before first work check", startup_delay_seconds)
        await asyncio.sleep(startup_delay_seconds)

    while True:
        try:
            had_work = await run_quarterly_update_cycle()
            if not had_work:
                await asyncio.sleep(max(no_work_sleep_seconds, 1.0))
        except asyncio.CancelledError:
            logger.info("[QUARTERLY UPDATE] Continuous worker cancelled")
            raise
        except Exception as error:
            logger.error("[QUARTERLY UPDATE] Continuous worker error: %s", error)
            await asyncio.sleep(max(error_sleep_seconds, 1.0))


def start_background_worker() -> None:
    """Start continuous quarterly worker if not already running."""
    global _worker_task
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(run_continuous_quarterly_worker())


async def stop_background_worker() -> None:
    """Stop continuous quarterly worker if running."""
    global _worker_task
    if _worker_task is not None and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass


def signal_handler(signum, _frame):
    """Handle shutdown signals gracefully."""
    logger.info("Received signal %s. Shutting down gracefully...", signum)
    close_db_connections()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        dev = os.getenv("DEV")
        if not dev:
            logger.info("Starting quarterly data fetcher worker (Worker ID: %s)", WORKER_ID)
            logger.info("Orchestrator URL: %s", ORCHESTRATOR_URL)
            logger.info("Quarterly update mode: continuous batch pull on startup")

            loop = asyncio.get_event_loop()
            loop.create_task(run_continuous_quarterly_worker())
            loop.run_forever()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
        close_db_connections()
    except Exception as error:
        logger.error("Unexpected error: %s", error)
        close_db_connections()
        raise

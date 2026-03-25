import logging

from fastapi import FastAPI

from quarterly_data_fetcher.logging_config import setup_logging
from quarterly_data_fetcher.worker_scheduler import (
    run_startup_quarterly_update,
    schedule_quarterly_task,
    scheduler,
)

app = FastAPI()
logger = setup_logging("quarterly-data-fetcher", level=logging.INFO)


@app.get("/")
def read_root():
    return {"message": "Hello FastAPI"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.on_event("startup")
async def startup_event():
    if not scheduler.running:
        schedule_quarterly_task()
        logger.info("Quarterly data fetcher scheduler started")

    await run_startup_quarterly_update()


@app.on_event("shutdown")
async def shutdown_event():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Quarterly data fetcher scheduler stopped")

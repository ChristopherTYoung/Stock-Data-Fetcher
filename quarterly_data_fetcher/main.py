import logging

from fastapi import FastAPI

from quarterly_data_fetcher.logging_config import setup_logging
from quarterly_data_fetcher.worker_scheduler import (
    start_background_worker,
    stop_background_worker,
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
    start_background_worker()
    logger.info("Quarterly data fetcher continuous worker started")


@app.on_event("shutdown")
async def shutdown_event():
    await stop_background_worker()
    logger.info("Quarterly data fetcher continuous worker stopped")

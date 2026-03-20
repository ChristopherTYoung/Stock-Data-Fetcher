from fastapi import FastAPI
import logging

from stock_data_calculator.worker_scheduler import scheduler, schedule_calculation_task

app = FastAPI()
logger = logging.getLogger(__name__)

@app.get("/")
def read_root():
    return {"message": "Hello FastAPI"}

@app.get("/health")
def health():
    return {"status": "ok"}


@app.on_event("startup")
async def startup_event():
    if not scheduler.running:
        schedule_calculation_task()
        logger.info("Stock calculator worker scheduler started")


@app.on_event("shutdown")
async def shutdown_event():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Stock calculator worker scheduler stopped")
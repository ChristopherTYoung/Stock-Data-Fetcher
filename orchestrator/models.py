"""Pydantic models for orchestrator API."""
from pydantic import BaseModel
from typing import List, Optional, Dict


class StockBatchResponse(BaseModel):
    """Response model for stock batch allocation."""
    tickers: List[str]
    batch_size: int
    remaining_in_queue: int
    total_processed: int
    timestamp: str


class OrchestratorStatus(BaseModel):
    """Status of the orchestrator."""
    total_stocks: int
    history_updates: Dict[str, int]
    gap_detection: Dict[str, int]
    last_refresh: Optional[str]
    next_refresh: Optional[str]

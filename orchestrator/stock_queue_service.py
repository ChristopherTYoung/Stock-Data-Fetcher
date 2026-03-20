"""Service for managing stock queues and batch allocation."""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import threading
import logging
from models import StockBatchResponse, OrchestratorStatus
from logging_config import setup_logging

logger = setup_logging("stock-orchestrator", level=logging.INFO)


class StockQueueService:
    """Manages stock queues for history updates and gap detection."""
    
    def __init__(self, stocks_per_request: int = 250, refresh_interval_hours: int = 24):
        self.stocks_per_request = stocks_per_request
        self.refresh_interval_hours = refresh_interval_hours
        
        # History queue state
        self.history_queue: List[str] = []
        self.history_processed: List[str] = []
        self.history_lock = threading.Lock()
        
        # Gap detection queue state
        self.gap_detection_queue: List[str] = []
        self.gap_detection_processed: List[str] = []
        self.gap_detection_lock = threading.Lock()

        # Stock calculation queue state
        self.stock_calculation_queue: List[str] = []
        self.stock_calculation_processed: List[str] = []
        self.stock_calculation_lock = threading.Lock()
        
        self.last_refresh_time: Optional[datetime] = None
    
    def refresh_queues(self, tickers: List[str]):
        """Refresh both queues with new stock list."""
        with self.history_lock:
            self.history_queue = tickers.copy()
            self.history_processed = []

        with self.gap_detection_lock:
            self.gap_detection_queue = tickers.copy()
            self.gap_detection_processed = []

        with self.stock_calculation_lock:
            self.stock_calculation_queue = tickers.copy()
            self.stock_calculation_processed = []
        
        self.last_refresh_time = datetime.now()
        
        logger.info(f"All queues refreshed with {len(tickers)} tickers")
    
    def get_batch(self, worker_id: Optional[str] = None) -> StockBatchResponse:
        """Get batch of stocks for history updates."""
        with self.history_lock:
            if not self.history_queue:
                logger.info(f"No stocks remaining in history queue (Worker: {worker_id})")
                return StockBatchResponse(
                    tickers=[],
                    batch_size=0,
                    remaining_in_queue=0,
                    total_processed=len(self.history_processed),
                    timestamp=datetime.now().isoformat()
                )

            batch_size = min(self.stocks_per_request, len(self.history_queue))
            batch = self.history_queue[:batch_size]
            
            self.history_queue[:batch_size] = []
            self.history_processed.extend(batch)
            
            logger.info(
                f"[HISTORY] Allocated {len(batch)} stocks to worker {worker_id or 'unknown'}. "
                f"Remaining: {len(self.history_queue)}, Processed: {len(self.history_processed)}"
            )
            
            return StockBatchResponse(
                tickers=batch,
                batch_size=len(batch),
                remaining_in_queue=len(self.history_queue),
                total_processed=len(self.history_processed),
                timestamp=datetime.now().isoformat()
            )
    
    def get_gap_detection_batch(self, worker_id: Optional[str] = None) -> StockBatchResponse:
        """Get batch of stocks for gap detection."""
        with self.gap_detection_lock:
            if not self.gap_detection_queue:
                logger.info(f"No stocks remaining in gap detection queue (Worker: {worker_id})")
                return StockBatchResponse(
                    tickers=[],
                    batch_size=0,
                    remaining_in_queue=0,
                    total_processed=len(self.gap_detection_processed),
                    timestamp=datetime.now().isoformat()
                )

            batch_size = min(self.stocks_per_request, len(self.gap_detection_queue))
            batch = self.gap_detection_queue[:batch_size]
            
            self.gap_detection_queue[:batch_size] = []
            self.gap_detection_processed.extend(batch)
            
            logger.info(
                f"[GAP DETECTION] Allocated {len(batch)} stocks to worker {worker_id or 'unknown'}. "
                f"Remaining: {len(self.gap_detection_queue)}, Processed: {len(self.gap_detection_processed)}"
            )
            
            return StockBatchResponse(
                tickers=batch,
                batch_size=len(batch),
                remaining_in_queue=len(self.gap_detection_queue),
                total_processed=len(self.gap_detection_processed),
                timestamp=datetime.now().isoformat()
            )

    def get_stock_calculation_batch(self, worker_id: Optional[str] = None) -> StockBatchResponse:
        """Get batch of stocks for stock-data calculation workers."""
        with self.stock_calculation_lock:
            if not self.stock_calculation_queue:
                logger.info(f"No stocks remaining in stock calculation queue (Worker: {worker_id})")
                return StockBatchResponse(
                    tickers=[],
                    batch_size=0,
                    remaining_in_queue=0,
                    total_processed=len(self.stock_calculation_processed),
                    timestamp=datetime.now().isoformat()
                )

            batch_size = min(self.stocks_per_request, len(self.stock_calculation_queue))
            batch = self.stock_calculation_queue[:batch_size]

            self.stock_calculation_queue[:batch_size] = []
            self.stock_calculation_processed.extend(batch)

            logger.info(
                f"[STOCK CALCULATION] Allocated {len(batch)} stocks to worker {worker_id or 'unknown'}. "
                f"Remaining: {len(self.stock_calculation_queue)}, Processed: {len(self.stock_calculation_processed)}"
            )

            return StockBatchResponse(
                tickers=batch,
                batch_size=len(batch),
                remaining_in_queue=len(self.stock_calculation_queue),
                total_processed=len(self.stock_calculation_processed),
                timestamp=datetime.now().isoformat()
            )
    
    def get_status(self) -> OrchestratorStatus:
        """Get current status of queues."""
        with self.history_lock:
            history_remaining = len(self.history_queue)
            history_total_processed = len(self.history_processed)
        
        with self.gap_detection_lock:
            gap_remaining = len(self.gap_detection_queue)
            gap_total_processed = len(self.gap_detection_processed)

        with self.stock_calculation_lock:
            stock_calculation_remaining = len(self.stock_calculation_queue)
            stock_calculation_total_processed = len(self.stock_calculation_processed)
        
        next_refresh = None
        if self.last_refresh_time:
            next_refresh = (self.last_refresh_time + timedelta(hours=self.refresh_interval_hours)).isoformat()
        
        total_stocks = history_remaining + history_total_processed
        
        return OrchestratorStatus(
            total_stocks=total_stocks,
            history_updates={
                "remaining": history_remaining,
                "processed": history_total_processed
            },
            gap_detection={
                "remaining": gap_remaining,
                "processed": gap_total_processed
            },
            stock_calculation={
                "remaining": stock_calculation_remaining,
                "processed": stock_calculation_total_processed
            },
            last_refresh=self.last_refresh_time.isoformat() if self.last_refresh_time else None,
            next_refresh=next_refresh
        )
    
    def reset_queues(self) -> Dict[str, Any]:
        """Reset processed stocks back to queues."""
        with self.history_lock:
            self.history_queue.extend(self.history_processed)
            self.history_processed.clear()
        
        with self.gap_detection_lock:
            self.gap_detection_queue.extend(self.gap_detection_processed)
            self.gap_detection_processed.clear()

        with self.stock_calculation_lock:
            self.stock_calculation_queue.extend(self.stock_calculation_processed)
            self.stock_calculation_processed.clear()
        
        logger.info(
            "Queues reset. History: %s, Gap detection: %s, Stock calculation: %s",
            len(self.history_queue),
            len(self.gap_detection_queue),
            len(self.stock_calculation_queue),
        )
        
        return {
            "success": True,
            "message": "All queues reset successfully",
            "history_queue": len(self.history_queue),
            "gap_detection_queue": len(self.gap_detection_queue),
            "stock_calculation_queue": len(self.stock_calculation_queue),
            "timestamp": datetime.now().isoformat()
        }

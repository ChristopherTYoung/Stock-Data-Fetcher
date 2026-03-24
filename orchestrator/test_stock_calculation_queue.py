from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent))

from stock_queue_service import StockQueueService


def test_stock_calculation_queue_refresh_and_batch():
    service = StockQueueService(stocks_per_request=2, refresh_interval_hours=24)
    tickers = ["AAPL", "MSFT", "GOOGL"]

    service.refresh_queues(tickers)

    status = service.get_status()
    assert status.stock_calculation["remaining"] == 3
    assert status.stock_calculation["processed"] == 0

    batch = service.get_stock_calculation_batch(worker_id="calc-worker")
    assert batch.tickers == ["AAPL", "MSFT"]
    assert batch.batch_size == 2
    assert batch.remaining_in_queue == 1
    assert batch.total_processed == 2

    status_after = service.get_status()
    assert status_after.stock_calculation["remaining"] == 1
    assert status_after.stock_calculation["processed"] == 2


def test_stock_calculation_queue_reset_restores_processed():
    service = StockQueueService(stocks_per_request=10, refresh_interval_hours=24)
    tickers = ["AAPL", "MSFT"]

    service.refresh_queues(tickers)
    service.get_stock_calculation_batch(worker_id="calc-worker")

    status_mid = service.get_status()
    assert status_mid.stock_calculation["remaining"] == 0
    assert status_mid.stock_calculation["processed"] == 2

    reset_result = service.reset_queues()
    assert reset_result["stock_calculation_queue"] == 2

    status_end = service.get_status()
    assert status_end.stock_calculation["remaining"] == 2
    assert status_end.stock_calculation["processed"] == 0


def test_quarterly_update_queue_refresh_and_batch():
    service = StockQueueService(stocks_per_request=2, refresh_interval_hours=24)
    tickers = ["AAPL", "MSFT", "GOOGL"]
    quarterly_tickers = ["AAPL", "GOOGL"]

    service.refresh_queues(tickers, quarterly_update_tickers=quarterly_tickers)

    status = service.get_status()
    assert status.quarterly_update["remaining"] == 2
    assert status.quarterly_update["processed"] == 0

    batch = service.get_quarterly_update_batch(worker_id="quarterly-worker")
    assert batch.tickers == ["AAPL", "GOOGL"]
    assert batch.batch_size == 2
    assert batch.remaining_in_queue == 0
    assert batch.total_processed == 2


def test_quarterly_update_queue_reset_restores_processed():
    service = StockQueueService(stocks_per_request=10, refresh_interval_hours=24)
    tickers = ["AAPL", "MSFT"]
    quarterly_tickers = ["AAPL"]

    service.refresh_queues(tickers, quarterly_update_tickers=quarterly_tickers)
    service.get_quarterly_update_batch(worker_id="quarterly-worker")

    status_mid = service.get_status()
    assert status_mid.quarterly_update["remaining"] == 0
    assert status_mid.quarterly_update["processed"] == 1

    reset_result = service.reset_queues()
    assert reset_result["quarterly_update_queue"] == 1

    status_end = service.get_status()
    assert status_end.quarterly_update["remaining"] == 1
    assert status_end.quarterly_update["processed"] == 0

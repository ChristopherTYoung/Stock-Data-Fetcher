import asyncio
from datetime import datetime

import worker_scheduler


def test_process_stock_batch_fetches_history_only(monkeypatch):
    calls = []

    class FakeFetcher:
        def fetch_all_tickers_historical_data(self, tickers, end_date):
            calls.append(("history", tickers, end_date))
            return {
                "summary": {
                    "successful": len(tickers),
                    "failed": 0,
                    "total_rows_inserted": 10,
                    "failed_tickers": [],
                }
            }

    def _fail_if_called():
        raise AssertionError("process_stock_batch should not call asyncio.get_running_loop")

    monkeypatch.setattr(worker_scheduler, "data_fetcher", FakeFetcher())
    monkeypatch.setattr(worker_scheduler.asyncio, "get_running_loop", _fail_if_called)

    tickers = ["AAPL", "MSFT"]
    asyncio.run(worker_scheduler.process_stock_batch(tickers))

    assert len(calls) == 1
    assert calls[0][0] == "history"
    assert calls[0][1] == tickers
    assert isinstance(calls[0][2], datetime)

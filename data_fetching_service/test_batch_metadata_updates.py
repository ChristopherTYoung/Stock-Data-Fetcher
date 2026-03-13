import asyncio
from datetime import datetime
from pathlib import Path
import sys
import types

sys.path.append(str(Path(__file__).resolve().parent))

if "polygon" not in sys.modules:
    polygon_stub = types.ModuleType("polygon")

    class _DummyRESTClient:
        pass

    polygon_stub.RESTClient = _DummyRESTClient
    sys.modules["polygon"] = polygon_stub

import polygon_stock_service
import worker_scheduler


def test_update_metadata_for_tickers_normalizes_input(monkeypatch):
    captured = {}

    def fake_update(stock_data, status_dict=None):
        captured["stock_data"] = stock_data
        return len(stock_data)

    monkeypatch.setattr(
        polygon_stock_service,
        "update_stocks_in_db_from_polygon",
        fake_update,
    )

    result = polygon_stock_service.update_metadata_for_tickers(
        [" aapl ", "", None, "msft", "AAPL"]
    )

    assert result == 3
    assert captured["stock_data"] == [
        {"symbol": "AAPL"},
        {"symbol": "MSFT"},
        {"symbol": "AAPL"},
    ]


def test_update_metadata_for_tickers_empty_returns_zero(monkeypatch):
    called = {"value": False}

    def fake_update(stock_data, status_dict=None):
        called["value"] = True
        return 999

    monkeypatch.setattr(
        polygon_stock_service,
        "update_stocks_in_db_from_polygon",
        fake_update,
    )

    assert polygon_stock_service.update_metadata_for_tickers([]) == 0
    assert called["value"] is False


def test_process_stock_batch_updates_metadata_then_history(monkeypatch):
    calls = []

    def fake_update_metadata_for_tickers(tickers):
        calls.append(("metadata", tickers))
        return len(tickers)

    class FakeFetcher:
        def __init__(self):
            self.called_with = None

        def fetch_all_tickers_historical_data(self, tickers, end_date):
            calls.append(("history", tickers, end_date))
            self.called_with = (tickers, end_date)
            return {
                "summary": {
                    "successful": len(tickers),
                    "failed": 0,
                    "total_rows_inserted": 10,
                    "failed_tickers": [],
                }
            }

    class FakeLoop:
        async def run_in_executor(self, _executor, fn, *args):
            return fn(*args)

    fake_fetcher = FakeFetcher()

    monkeypatch.setattr(worker_scheduler, "update_metadata_for_tickers", fake_update_metadata_for_tickers)
    monkeypatch.setattr(worker_scheduler, "data_fetcher", fake_fetcher)
    monkeypatch.setattr(worker_scheduler.asyncio, "get_running_loop", lambda: FakeLoop())

    tickers = ["AAPL", "MSFT"]
    asyncio.run(worker_scheduler.process_stock_batch(tickers))

    assert calls[0][0] == "history"
    assert calls[0][1] == tickers
    assert isinstance(calls[0][2], datetime)
    assert calls[1] == ("metadata", tickers)

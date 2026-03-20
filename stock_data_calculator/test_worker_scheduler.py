import asyncio

from stock_data_calculator import worker_scheduler


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeAsyncClient:
    def __init__(self, payload):
        self._payload = payload
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def post(self, url, params=None):
        self.calls.append((url, params))
        return _FakeResponse(self._payload)


def test_fetch_calculation_batch_from_orchestrator(monkeypatch):
    fake_payload = {"tickers": ["AAPL", "MSFT"], "remaining_in_queue": 10}
    fake_client = _FakeAsyncClient(fake_payload)

    monkeypatch.setattr(worker_scheduler.httpx, "AsyncClient", lambda timeout: fake_client)

    tickers = asyncio.run(worker_scheduler.fetch_calculation_batch_from_orchestrator())
    assert tickers == ["AAPL", "MSFT"]


def test_process_stock_calculation_batch_uses_executor(monkeypatch):
    calls = {}

    def fake_update(tickers):
        calls["tickers"] = tickers
        return len(tickers)

    class FakeLoop:
        async def run_in_executor(self, _executor, fn, *args):
            return fn(*args)

    monkeypatch.setattr(worker_scheduler, "update_metadata_for_tickers", fake_update)
    monkeypatch.setattr(worker_scheduler.asyncio, "get_running_loop", lambda: FakeLoop())

    asyncio.run(worker_scheduler.process_stock_calculation_batch(["AAPL", "MSFT"]))
    assert calls["tickers"] == ["AAPL", "MSFT"]

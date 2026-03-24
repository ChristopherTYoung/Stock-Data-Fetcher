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
    assert fake_client.calls[0][0].endswith("/get-stock-calculation-batch")


def test_fetch_quarterly_update_batch_from_orchestrator(monkeypatch):
    fake_payload = {"tickers": ["AAPL"], "remaining_in_queue": 5}
    fake_client = _FakeAsyncClient(fake_payload)

    monkeypatch.setattr(worker_scheduler.httpx, "AsyncClient", lambda timeout: fake_client)

    tickers = asyncio.run(worker_scheduler.fetch_quarterly_update_batch_from_orchestrator())
    assert tickers == ["AAPL"]
    assert fake_client.calls[0][0].endswith("/get-quarterly-update-batch")


def test_process_stock_calculation_batch_uses_executor(monkeypatch):
    calls = []

    def fake_update(tickers, status_dict=None):
        calls.append(("metadata", tickers))
        return len(tickers)

    monkeypatch.setattr(worker_scheduler, "update_metadata_for_tickers", fake_update)

    asyncio.run(worker_scheduler.process_stock_calculation_batch(["AAPL", "MSFT"]))
    assert calls == [("metadata", ["AAPL", "MSFT"])]


def test_run_calculation_cycle_processes_regular_updates(monkeypatch):
    processed = []

    async def fake_regular_fetch():
        return ["MSFT", "NVDA"]

    async def fake_process(tickers):
        processed.append(tickers)

    monkeypatch.setattr(worker_scheduler, "fetch_calculation_batch_from_orchestrator", fake_regular_fetch)
    monkeypatch.setattr(worker_scheduler, "process_stock_calculation_batch", fake_process)

    asyncio.run(worker_scheduler.run_calculation_cycle())

    assert processed == [["MSFT", "NVDA"]]


def test_run_quarterly_update_cycle_processes_quarterly_updates(monkeypatch):
    processed = []

    async def fake_quarterly_fetch():
        return ["AAPL"]

    async def fake_process(tickers):
        processed.append(tickers)

    monkeypatch.setattr(worker_scheduler, "fetch_quarterly_update_batch_from_orchestrator", fake_quarterly_fetch)
    monkeypatch.setattr(worker_scheduler, "process_quarterly_update_batch", fake_process)

    asyncio.run(worker_scheduler.run_quarterly_update_cycle())

    assert processed == [["AAPL"]]

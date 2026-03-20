from stock_data_calculator import polygon_stock_service


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

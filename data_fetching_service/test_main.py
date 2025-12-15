import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd
from main import (
    fetch_ticker_hourly_data,
    fetch_ticker_minute_data,
    fetch_all_tickers_historical_data,
    save_stock_data_to_db
)


@pytest.fixture
def mock_hourly_data():
    """Create mock hourly OHLCV data."""
    dates = pd.date_range(start='2023-12-01', end='2023-12-03', freq='1h')
    return pd.DataFrame({
        'Open': [100.0 + i for i in range(len(dates))],
        'High': [102.0 + i for i in range(len(dates))],
        'Low': [99.0 + i for i in range(len(dates))],
        'Close': [101.0 + i for i in range(len(dates))],
        'Volume': [1000000 + i * 1000 for i in range(len(dates))]
    }, index=dates)


@pytest.fixture
def mock_minute_data():
    """Create mock minute-level OHLCV data."""
    dates = pd.date_range(start='2023-12-01 09:30', end='2023-12-01 16:00', freq='1min')
    return pd.DataFrame({
        'Open': [100.0 + i * 0.1 for i in range(len(dates))],
        'High': [100.5 + i * 0.1 for i in range(len(dates))],
        'Low': [99.5 + i * 0.1 for i in range(len(dates))],
        'Close': [100.2 + i * 0.1 for i in range(len(dates))],
        'Volume': [10000 + i * 100 for i in range(len(dates))]
    }, index=dates)


class TestFetchTickerHourlyData:
    @patch('main.yf.download')
    def test_successful_hourly_fetch(self, mock_download, mock_hourly_data):
        """Test successful hourly data fetch."""
        mock_download.return_value = mock_hourly_data
        
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 3)
        
        result = fetch_ticker_hourly_data('AAPL', start_date, end_date)
        
        assert not result.empty
        assert len(result) == len(mock_hourly_data)
        mock_download.assert_called_once()
        
        # Verify correct parameters
        call_kwargs = mock_download.call_args.kwargs
        assert call_kwargs['tickers'] == 'AAPL'
        assert call_kwargs['interval'] == '1h'
        assert call_kwargs['auto_adjust'] is True
    
    @patch('main.yf.download')
    def test_empty_hourly_data(self, mock_download):
        """Test handling of empty data response."""
        mock_download.return_value = pd.DataFrame()
        
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 3)
        
        result = fetch_ticker_hourly_data('INVALID', start_date, end_date)
        
        assert result.empty
        # Should retry 3 times
        assert mock_download.call_count == 3
    
    @patch('main.yf.download')
    @patch('main.time.sleep')
    def test_retry_on_exception(self, mock_sleep, mock_download):
        """Test retry logic when exceptions occur."""
        mock_download.side_effect = [Exception("API Error"), Exception("API Error"), pd.DataFrame()]
        
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 3)
        
        result = fetch_ticker_hourly_data('AAPL', start_date, end_date)
        
        assert result.empty
        assert mock_download.call_count == 3
        assert mock_sleep.call_count == 2  # Sleep between retries


class TestFetchTickerMinuteData:
    """Tests for fetch_ticker_minute_data function."""
    
    @patch('main.yf.download')
    @patch('main.time.sleep')
    def test_successful_minute_fetch(self, mock_sleep, mock_download, mock_minute_data):
        """Test successful minute data fetch with chunking."""
        mock_download.return_value = mock_minute_data
        
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 15)  # 14 days, should be 3 chunks (7+7+1)
        
        result = fetch_ticker_minute_data('AAPL', start_date, end_date)
        
        assert not result.empty
        # Should fetch in 7-day chunks (3 chunks for 15 days)
        assert mock_download.call_count == 3
    
    @patch('main.yf.download')
    @patch('main.time.sleep')
    def test_minute_data_chunking(self, mock_sleep, mock_download, mock_minute_data):
        """Test that minute data is fetched in proper 7-day chunks."""
        mock_download.return_value = mock_minute_data
        
        start_date = datetime(2023, 11, 1)
        end_date = datetime(2023, 12, 1)  # 30 days
        
        result = fetch_ticker_minute_data('AAPL', start_date, end_date)
        
        # 30 days / 7 days per chunk = ~5 chunks
        assert mock_download.call_count >= 4
        assert mock_download.call_count <= 5
    
    @patch('main.yf.download')
    @patch('main.time.sleep')
    def test_minute_data_empty_result(self, mock_sleep, mock_download):
        """Test handling when all chunks return empty data."""
        mock_download.return_value = pd.DataFrame()
        
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 8)
        
        result = fetch_ticker_minute_data('INVALID', start_date, end_date)
        
        assert result.empty
    
    @patch('main.yf.download')
    @patch('main.time.sleep')
    def test_minute_data_retry_on_error(self, mock_sleep, mock_download, mock_minute_data):
        """Test retry logic for individual chunks."""
        # First chunk fails twice then succeeds, second chunk succeeds immediately
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise Exception("Error")
            return mock_minute_data
        
        mock_download.side_effect = side_effect
        
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2023, 12, 8)  # 7 days, 2 chunks
        
        result = fetch_ticker_minute_data('AAPL', start_date, end_date)
        
        assert not result.empty
        # 2 failed attempts + 1 success for chunk 1 + 1 success for chunk 2 = 4 total
        assert mock_download.call_count == 4


class TestSaveStockDataToDb:
    """Tests for save_stock_data_to_db function."""
    
    @patch('main.get_db')
    def test_save_hourly_data(self, mock_get_db, mock_hourly_data):
        """Test saving hourly data to database."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        rows_inserted = save_stock_data_to_db('AAPL', mock_hourly_data)
        
        assert rows_inserted == len(mock_hourly_data)
        assert mock_db.add.call_count == len(mock_hourly_data)
        mock_db.commit.assert_called_once()
    
    @patch('main.get_db')
    def test_save_minute_data(self, mock_get_db, mock_minute_data):
        """Test saving minute data to database."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        rows_inserted = save_stock_data_to_db('GOOGL', mock_minute_data)
        
        assert rows_inserted == len(mock_minute_data)
        assert mock_db.add.call_count == len(mock_minute_data)
        mock_db.commit.assert_called_once()
    
    @patch('main.get_db')
    def test_save_empty_dataframe(self, mock_get_db):
        """Test handling of empty dataframe."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        rows_inserted = save_stock_data_to_db('AAPL', pd.DataFrame())
        
        assert rows_inserted == 0
        mock_db.add.assert_not_called()
        mock_db.commit.assert_not_called()
    
    @patch('main.get_db')
    def test_handle_duplicate_entries(self, mock_get_db, mock_hourly_data):
        """Test handling when some rows fail to insert (e.g., duplicates)."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        # Make every other add() call raise an exception
        add_call_count = [0]
        def side_effect(record):
            add_call_count[0] += 1
            if add_call_count[0] % 2 == 0:
                raise Exception("Duplicate entry")
        
        mock_db.add.side_effect = side_effect
        
        rows_inserted = save_stock_data_to_db('AAPL', mock_hourly_data)
        
        # Should have tried to insert all rows but only odd-numbered ones succeeded
        # With 49 rows, odd numbers are 1,3,5,...,49 = 25 rows
        expected_successful = (len(mock_hourly_data) + 1) // 2
        assert rows_inserted == expected_successful
        mock_db.commit.assert_called_once()
    
    @patch('main.get_db')
    def test_price_conversion_to_cents(self, mock_get_db):
        """Test that prices are correctly converted to cents."""
        mock_db = MagicMock()
        mock_get_db.return_value.__enter__.return_value = mock_db
        
        df = pd.DataFrame({
            'Open': [100.50],
            'High': [102.75],
            'Low': [99.25],
            'Close': [101.00],
            'Volume': [1000000]
        }, index=[datetime(2023, 12, 1, 10, 0)])
        
        save_stock_data_to_db('AAPL', df)
        
        # Get the StockHistory object that was added
        added_record = mock_db.add.call_args[0][0]
        
        assert added_record.open == 10050  # 100.50 * 100
        assert added_record.high == 10275  # 102.75 * 100
        assert added_record.low == 9925    # 99.25 * 100
        assert added_record.close == 10100  # 101.00 * 100
        assert added_record.volume == 1000000


class TestFetchAllTickersHistoricalData:
    """Tests for fetch_all_tickers_historical_data function."""
    
    @patch('main.fetch_ticker_minute_data')
    @patch('main.fetch_ticker_hourly_data')
    @patch('main.save_stock_data_to_db')
    @patch('main.time.sleep')
    def test_fetch_multiple_tickers(self, mock_sleep, mock_save, mock_hourly, mock_minute, 
                                    mock_hourly_data, mock_minute_data):
        """Test fetching data for multiple tickers."""
        mock_hourly.return_value = mock_hourly_data
        mock_minute.return_value = mock_minute_data
        mock_save.return_value = 100
        
        tickers = ['AAPL', 'GOOGL', 'MSFT']
        end_date = datetime.now()
        
        result = fetch_all_tickers_historical_data(tickers, end_date)
        
        assert result['summary']['total_tickers'] == 3
        assert result['summary']['successful'] == 3
        assert result['summary']['failed'] == 0
        assert len(result['summary']['failed_tickers']) == 0
        
        # Should fetch both hourly and minute data for each ticker
        assert mock_hourly.call_count == 3
        assert mock_minute.call_count == 3
        assert mock_save.call_count == 6  # 3 tickers * 2 data types
    
    @patch('main.fetch_ticker_minute_data')
    @patch('main.fetch_ticker_hourly_data')
    @patch('main.save_stock_data_to_db')
    @patch('main.time.sleep')
    def test_correct_date_ranges(self, mock_sleep, mock_save, mock_hourly, mock_minute,
                                 mock_hourly_data, mock_minute_data):
        """Test that correct date ranges are used."""
        mock_hourly.return_value = mock_hourly_data
        mock_minute.return_value = mock_minute_data
        mock_save.return_value = 50
        
        end_date = datetime(2024, 1, 1)
        
        result = fetch_all_tickers_historical_data(['AAPL'], end_date)
        
        # Check hourly data call (2 years back)
        hourly_call = mock_hourly.call_args
        hourly_start = hourly_call[0][1]
        expected_hourly_start = end_date - timedelta(days=2 * 365)
        assert hourly_start.date() == expected_hourly_start.date()
        
        # Check minute data call (1 month back)
        minute_call = mock_minute.call_args
        minute_start = minute_call[0][1]
        expected_minute_start = end_date - timedelta(days=30)
        assert minute_start.date() == expected_minute_start.date()
    
    @patch('main.fetch_ticker_minute_data')
    @patch('main.fetch_ticker_hourly_data')
    @patch('main.save_stock_data_to_db')
    @patch('main.time.sleep')
    def test_handle_partial_failures(self, mock_sleep, mock_save, mock_hourly, mock_minute,
                                     mock_hourly_data):
        """Test handling when some tickers fail."""
        # First ticker succeeds, second fails, third succeeds
        mock_hourly.side_effect = [mock_hourly_data, pd.DataFrame(), mock_hourly_data]
        mock_minute.side_effect = [mock_hourly_data, pd.DataFrame(), mock_hourly_data]
        mock_save.return_value = 100
        
        tickers = ['AAPL', 'INVALID', 'MSFT']
        end_date = datetime.now()
        
        result = fetch_all_tickers_historical_data(tickers, end_date)
        
        assert result['summary']['total_tickers'] == 3
        assert result['summary']['successful'] == 2
        assert result['summary']['failed'] == 1
        assert 'INVALID' in result['summary']['failed_tickers']
        assert result['results']['AAPL']['success'] is True
        assert result['results']['INVALID']['success'] is False
        assert result['results']['MSFT']['success'] is True
    
    @patch('main.rate_limited', True)
    def test_rate_limited_behavior(self):
        """Test that function returns early when rate limited."""
        tickers = ['AAPL', 'GOOGL']
        end_date = datetime.now()
        
        result = fetch_all_tickers_historical_data(tickers, end_date)
        
        assert result['summary']['total_tickers'] == 2
        assert result['summary']['successful'] == 0
        assert result['summary']['failed'] == 2
        assert 'error' in result['summary']
        assert result['summary']['error'] == 'Service is rate limited'
    
    @patch('main.fetch_ticker_minute_data')
    @patch('main.fetch_ticker_hourly_data')
    @patch('main.save_stock_data_to_db')
    @patch('main.time.sleep')
    def test_exception_handling(self, mock_sleep, mock_save, mock_hourly, mock_minute):
        """Test handling of exceptions during processing."""
        mock_hourly.side_effect = Exception("Network error")
        
        tickers = ['AAPL']
        end_date = datetime.now()
        
        result = fetch_all_tickers_historical_data(tickers, end_date)
        
        assert result['summary']['failed'] == 1
        assert 'AAPL' in result['summary']['failed_tickers']
        assert result['results']['AAPL']['success'] is False
        assert 'error' in result['results']['AAPL']
    
    @patch('main.fetch_ticker_minute_data')
    @patch('main.fetch_ticker_hourly_data')
    @patch('main.save_stock_data_to_db')
    @patch('main.time.sleep')
    def test_row_count_tracking(self, mock_sleep, mock_save, mock_hourly, mock_minute,
                                mock_hourly_data, mock_minute_data):
        """Test that row counts are correctly tracked."""
        mock_hourly.return_value = mock_hourly_data
        mock_minute.return_value = mock_minute_data
        mock_save.side_effect = [150, 50]  # 150 hourly, 50 minute
        
        result = fetch_all_tickers_historical_data(['AAPL'], datetime.now())
        
        assert result['results']['AAPL']['rows_inserted'] == 200
        assert result['results']['AAPL']['hourly_rows'] == len(mock_hourly_data)
        assert result['results']['AAPL']['minute_rows'] == len(mock_minute_data)
        assert result['summary']['total_rows_inserted'] == 200


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

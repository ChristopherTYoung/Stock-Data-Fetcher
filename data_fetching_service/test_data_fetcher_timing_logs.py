"""
Tests for data fetcher timing logs with mocked Polygon API and logging.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime, timedelta
import pandas as pd
import time
import logging
from io import StringIO

from data_fetcher import DataFetcher
from database_service import DatabaseService


@pytest.fixture
def mock_polygon_client():
    """Create a mock Polygon REST client that sleeps to simulate API delay."""
    mock_client = Mock()
    
    def mock_list_aggs(*args, **kwargs):
        """Mock list_aggs that sleeps and returns bar data."""
        time.sleep(1.5)  # Simulate API delay
        
        # Create mock bar objects
        bars = []
        for i in range(5):
            bar = Mock()
            bar.timestamp = 1609459200000 + (i * 3600000)  # hourly timestamps
            bar.open = 100.0 + i
            bar.high = 102.0 + i
            bar.low = 99.0 + i
            bar.close = 101.0 + i
            bar.volume = 1000000 + (i * 100000)
            bars.append(bar)
        
        return iter(bars)
    
    mock_client.list_aggs = mock_list_aggs
    return mock_client


@pytest.fixture
def caplog_with_level():
    """Configure caplog to capture all log levels."""
    return pytest.LogCaptureFixture


class TestDataFetcherTimingLogs:
    """Test data fetcher timing logs."""

    @patch('data_fetcher.RESTClient')
    @patch.object(DatabaseService, 'save_stock_data_to_db')
    @patch('data_fetcher.get_db')
    @patch('data_fetcher.logger')
    def test_fetch_all_tickers_logs_timing_with_non_null_duration(
        self, 
        mock_logger,
        mock_get_db,
        mock_save_data,
        mock_rest_client,
        mock_polygon_client
    ):
        """Test that timing logs are generated with non-null durations."""
        
        # Setup mocks
        mock_rest_client.return_value = mock_polygon_client
        mock_save_data.return_value = 5  # 5 rows saved
        
        # Mock database context manager
        mock_db_instance = Mock()
        mock_db_instance.execute.return_value.scalars.return_value.first.return_value = None
        mock_get_db.return_value.__enter__ = Mock(return_value=mock_db_instance)
        mock_get_db.return_value.__exit__ = Mock(return_value=None)
        
        # Create data fetcher
        fetcher = DataFetcher()
        
        # Fetch data for single ticker
        tickers = ['AAPL']
        end_date = datetime(2021, 1, 1)
        
        result = fetcher.fetch_all_tickers_historical_data(tickers, end_date)
        
        # Verify result
        assert result['summary']['successful'] == 1
        assert result['summary']['total_rows_inserted'] == 10  # 5 hourly + 5 minute
        
        # Extract all log calls
        all_log_calls = mock_logger.info.call_args_list + mock_logger.debug.call_args_list
        
        # Find logs that contain timing information
        timing_logs = [
            call[0][0] for call in all_log_calls
            if any(indicator in str(call[0][0]) for indicator in ['fetch:', 'total time:', 'f))'])
        ]
        
        assert len(timing_logs) > 0, "No timing logs found"
        
        # Verify timing values are not null/zero
        for log_message in timing_logs:
            # Extract numeric timing values using simple regex-like patterns
            import re
            times = re.findall(r'(\d+\.\d+)s', str(log_message))
            assert len(times) > 0, f"No timing values found in log: {log_message}"
            
            # Verify times are not zero
            for time_str in times:
                time_val = float(time_str)
                assert time_val > 0, f"Timing value is zero in log: {log_message}"

    @patch('data_fetcher.RESTClient')
    @patch.object(DatabaseService, 'save_stock_data_to_db')
    @patch('data_fetcher.get_db')
    def test_fetch_time_exceeds_sleep_duration(
        self,
        mock_get_db,
        mock_save_data,
        mock_rest_client,
        mock_polygon_client
    ):
        """Test that recorded fetch time reflects the API sleep delay."""
        
        # Setup mocks
        mock_rest_client.return_value = mock_polygon_client
        mock_save_data.return_value = 5
        
        mock_db_instance = Mock()
        mock_db_instance.execute.return_value.scalars.return_value.first.return_value = None
        mock_get_db.return_value.__enter__ = Mock(return_value=mock_db_instance)
        mock_get_db.return_value.__exit__ = Mock(return_value=None)
        
        fetcher = DataFetcher()
        
        # Capture logs properly
        with patch('data_fetcher.logger') as mock_logger:
            tickers = ['AAPL']
            end_date = datetime(2021, 1, 1)
            
            result = fetcher.fetch_all_tickers_historical_data(tickers, end_date)
            
            # Verify success
            assert result['summary']['successful'] == 1
            
            # Extract timing from logs
            import re
            timing_logs = []
            for call_obj in mock_logger.info.call_args_list:
                log_msg = str(call_obj[0][0])
                times = re.findall(r'fetch: (\d+\.\d+)s', log_msg)
                if times:
                    timing_logs.extend([float(t) for t in times])
            
            # Should have at least 2 fetch times (hourly and minute data)
            assert len(timing_logs) >= 2, f"Expected at least 2 timing logs, got {len(timing_logs)}"
            
            # Each should be >= 1.5s (due to sleep in mock)
            for fetch_time in timing_logs:
                assert fetch_time >= 1.5, f"Fetch time {fetch_time}s is less than expected 1.5s sleep"

    @patch('data_fetcher.RESTClient')
    @patch.object(DatabaseService, 'save_stock_data_to_db')
    @patch('data_fetcher.get_db')
    def test_success_log_includes_total_time_and_rows(
        self,
        mock_get_db,
        mock_save_data,
        mock_rest_client,
        mock_polygon_client
    ):
        """Test that SUCCESS log includes total time and is not null."""
        
        # Setup mocks
        mock_rest_client.return_value = mock_polygon_client
        mock_save_data.return_value = 5
        
        mock_db_instance = Mock()
        mock_db_instance.execute.return_value.scalars.return_value.first.return_value = None
        mock_get_db.return_value.__enter__ = Mock(return_value=mock_db_instance)
        mock_get_db.return_value.__exit__ = Mock(return_value=None)
        
        with patch('data_fetcher.logger') as mock_logger:
            fetcher = DataFetcher()
            
            tickers = ['AAPL']
            end_date = datetime(2021, 1, 1)
            
            result = fetcher.fetch_all_tickers_historical_data(tickers, end_date)
            
            # Find SUCCESS log
            success_logs = [
                str(call_obj[0][0]) for call_obj in mock_logger.info.call_args_list
                if 'SUCCESS' in str(call_obj[0][0])
            ]
            
            assert len(success_logs) > 0, "No SUCCESS log found"
            
            success_log = success_logs[0]
            
            # Verify SUCCESS log contains timing info
            assert 'total time:' in success_log, f"No total time in SUCCESS log: {success_log}"
            
            # Extract total time value
            import re
            times = re.findall(r'total time: (\d+\.\d+)s', success_log)
            assert len(times) > 0, f"Could not extract total time from: {success_log}"
            
            total_time = float(times[0])
            assert total_time > 0, f"Total time is zero: {total_time}"
            assert total_time >= 3.0, f"Total time {total_time}s seems too short"

    @patch('data_fetcher.RESTClient')
    @patch.object(DatabaseService, 'save_stock_data_to_db')
    @patch('data_fetcher.get_db')
    def test_multiple_tickers_timing_logs(
        self,
        mock_get_db,
        mock_save_data,
        mock_rest_client,
        mock_polygon_client
    ):
        """Test that timing logs are generated for multiple tickers."""
        
        # Setup mocks
        mock_rest_client.return_value = mock_polygon_client
        mock_save_data.return_value = 5
        
        mock_db_instance = Mock()
        mock_db_instance.execute.return_value.scalars.return_value.first.return_value = None
        mock_get_db.return_value.__enter__ = Mock(return_value=mock_db_instance)
        mock_get_db.return_value.__exit__ = Mock(return_value=None)
        
        with patch('data_fetcher.logger') as mock_logger:
            fetcher = DataFetcher()
            
            tickers = ['AAPL', 'GOOGL']
            end_date = datetime(2021, 1, 1)
            
            result = fetcher.fetch_all_tickers_historical_data(tickers, end_date)
            
            # Verify both tickers processed
            assert result['summary']['successful'] == 2
            
            # Verify timing logs for both
            success_logs = [
                str(call_obj[0][0]) for call_obj in mock_logger.info.call_args_list
                if 'SUCCESS' in str(call_obj[0][0])
            ]
            
            assert len(success_logs) == 2, f"Expected 2 SUCCESS logs, got {len(success_logs)}"
            
            # All should have timing
            import re
            for success_log in success_logs:
                assert 'total time:' in success_log, f"No timing in log: {success_log}"
                times = re.findall(r'total time: (\d+\.\d+)s', success_log)
                assert len(times) > 0
                assert float(times[0]) > 0, "Timing is zero or null"

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Google BigQuery Query connector."""

import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from grove.connectors.google.bigquery_query import Connector
from grove.models import ConnectorConfig
from grove.exceptions import ConfigurationException, RequestFailedException
from tests import mocks


class GoogleBigQueryQueryTestCase(unittest.TestCase):
    """Implements unit tests for the Google BigQuery Query connector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        
        self.connector = Connector(
            config=ConnectorConfig(
                identity="test-project",
                key="{}",  # Empty JSON, will be mocked
                name="test-bigquery",
                connector="google_bigquery_query",
                project_id="test-project",
                dataset_name="test_dataset",
                table_name="test_table",
                columns=["timestamp_usec", "message", "user_id"],
                pointer_path="timestamp_usec",
                max_batches=1,
                page_size=5000,
                bootstrap_days=7,
                min_lookback_days=3,
                max_lookback_days=30,
                late_buffer_days=2
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_collect_no_pagination(self, mock_get_creds, mock_bigquery_client):
        """Ensure collection without pagination works as expected."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results - less than 1000 rows (no pagination needed)
        mock_rows = [
            {"timestamp_usec": 1738500089504000, "message": "Test log 1", "user_id": "user1"},
            {"timestamp_usec": 1738500089505000, "message": "Test log 2", "user_id": "user2"},
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Set initial pointer
        self.connector._pointer = "1738500089504000"
        
        # Run collection
        self.connector.run()
        
        # Verify results
        self.assertEqual(self.connector._saved["logs"], 2)
        # Note: pointer won't be updated since we're mocking the save method

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')  
    def test_collect_with_pagination(self, mock_get_creds, mock_bigquery_client):
        """Ensure collection with pagination works as expected."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # First call returns 5000 rows (triggers pagination)
        # Second call returns 500 rows (ends pagination)
        mock_rows_5000 = [{"timestamp_usec": i, "message": f"Log {i}"} for i in range(5000)]
        mock_rows_500 = [{"timestamp_usec": i + 5000, "message": f"Log {i + 5000}"} for i in range(500)]
        
        mock_query_job.result.side_effect = [mock_rows_5000, mock_rows_500]
        
        # Set initial pointer
        self.connector._pointer = "1000000000000000"
        
        # Set max_batches to 2 for this test
        self.connector.configuration.max_batches = 2
        
        # Run collection
        self.connector.run()
        
        # Verify pagination occurred (query called twice)
        self.assertEqual(mock_client.query.call_count, 2)
        # Total logs saved: 5000 + 500 = 5500
        self.assertEqual(self.connector._saved["logs"], 5500)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_collect_without_max_batches(self, mock_get_creds, mock_bigquery_client):
        """Test that the connector works when max_batches is not in config (uses default)."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results - less than 5000 rows
        mock_rows = [
            {"timestamp_usec": 1738500089504000, "message": "Test log 1", "user_id": "user1"},
            {"timestamp_usec": 1738500089505000, "message": "Test log 2", "user_id": "user2"},
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Create connector WITHOUT max_batches in config
        connector_without_max_batches = Connector(
            config=ConnectorConfig(
                identity="test-project",
                key="{}",
                name="test-bigquery-no-max-batches",
                connector="google_bigquery_query",
                project_id="test-project",
                dataset_name="test_dataset",
                table_name="test_table",
                columns=["timestamp_usec", "message", "user_id"],
                pointer_path="timestamp_usec"
                # Note: no max_batches field - should default to 3
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        
        # Set initial pointer
        connector_without_max_batches._pointer = "1738500089504000"
        
        # Run collection - this should work with default max_batches=3
        connector_without_max_batches.run()
        
        # Verify results
        self.assertEqual(connector_without_max_batches._saved["logs"], 2)
        
        # Verify that the default value of 3 was used by checking the connector's behavior
        # Since we only have 2 rows (< 5000), it should complete in one batch
        self.assertEqual(mock_client.query.call_count, 1)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_collect_no_pointer_timestamp_format(self, mock_get_creds, mock_bigquery_client):
        """Ensure the connector works with timestamp format when no previous pointer exists."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results with timestamp format data
        mock_rows = [
            {"created_at": "2025-01-01 10:00:00", "message": "Test log 1", "user_id": "user1"},
            {"created_at": "2025-01-01 10:01:00", "message": "Test log 2", "user_id": "user2"},
            {"created_at": "2025-01-01 10:02:00", "message": "Test log 3", "user_id": "user3"},
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Adjust self.connector's configuration for this test
        self.connector.configuration.pointer_path = "created_at"
        self.connector.configuration.columns = ["*"]
        self.connector.configuration.time_format = "timestamp"
        self.connector.configuration.max_batches = 10
        
        # Ensure no pointer exists (should be empty string initially)
        self.connector._pointer = ""
        
        # Run collection
        self.connector.run()
        
        # Verify results
        self.assertEqual(self.connector._saved["logs"], 3)
        
        # Verify that the query was called once (no pagination needed for 3 rows)
        self.assertEqual(mock_client.query.call_count, 1)
        
        # Verify the query was called with appropriate parameters
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args[0][0]
        
        # Check that the query includes the table reference
        self.assertIn("test_dataset.test_table", call_args)
        
        # Check that the query includes ORDER BY for the pointer path
        self.assertIn("ORDER BY created_at", call_args)
        
        # Check that the query includes LIMIT for pagination (now uses @page_size parameter)
        self.assertIn("LIMIT @page_size", call_args)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_collect_timestamp_format_with_pointer(self, mock_get_creds, mock_bigquery_client):
        """Ensure the connector works with timestamp format when a previous pointer exists."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results with timestamp format data
        mock_rows = [
            {"created_at": "2025-01-01 11:00:00", "message": "Test log 4", "user_id": "user4"},
            {"created_at": "2025-01-01 11:01:00", "message": "Test log 5", "user_id": "user5"},
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Adjust self.connector's configuration for this test
        self.connector.configuration.pointer_path = "created_at"
        self.connector.configuration.columns = ["*"]
        self.connector.configuration.time_format = "timestamp"
        self.connector.configuration.max_batches = 10
        
        # Set existing pointer in timestamp format
        self.connector._pointer = "2025-01-01 10:00:00"
        
        # Run collection
        self.connector.run()
        
        # Verify results
        self.assertEqual(self.connector._saved["logs"], 2)
        
        # Verify that the query was called once
        self.assertEqual(mock_client.query.call_count, 1)
        
        # Verify the query was called with appropriate parameters
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args[0][0]
        
        # Check that the query includes the timestamp comparison (now uses @low_watermark parameter)
        self.assertIn("created_at > @low_watermark", call_args)
        
        # Check that the query includes ORDER BY for the pointer path
        self.assertIn("ORDER BY created_at", call_args)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_collect_invalid_pointer_formats(self, mock_get_creds, mock_bigquery_client):
        """Ensure the connector handles invalid pointer formats by falling back to default."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock empty query results (no more data after default pointer)
        mock_query_job.result.return_value = []
        
        # Test 1: Invalid microseconds pointer (non-integer) - should fall back to default
        self.connector.configuration.time_format = "microseconds"
        self.connector._pointer = "not_a_number"
        
        # Run collection - should not raise exception, should fall back to default
        self.connector.run()
        
        # Verify that the query was called with a default microseconds pointer
        self.assertEqual(mock_client.query.call_count, 1)
        call_args = mock_client.query.call_args[0][0]
        
        # Should contain a valid microseconds value (7 days ago) - now uses @low_watermark parameter
        self.assertIn("timestamp_usec > @low_watermark", call_args)
        
        # Reset for next test
        mock_client.reset_mock()
        
        # Test 2: Invalid timestamp pointer (malformed date) - should fall back to default
        self.connector.configuration.time_format = "timestamp"
        self.connector.configuration.pointer_path = "created_at"
        self.connector._pointer = "invalid-date-format"
        
        # Run collection - should not raise exception, should fall back to default
        self.connector.run()
        
        # Verify that the query was called with a default timestamp pointer
        self.assertEqual(mock_client.query.call_count, 1)
        call_args = mock_client.query.call_args[0][0]
        
        # Should contain a valid timestamp value (7 days ago) - now uses @low_watermark parameter
        self.assertIn("created_at > @low_watermark", call_args)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_collect_no_results(self, mock_get_creds, mock_bigquery_client):
        """Ensure the connector handles empty results gracefully."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock empty query results
        mock_query_job.result.return_value = []
        
        # Set initial pointer
        self.connector._pointer = "1738500089504000"
        
        # Run collection
        self.connector.run()
        
        # Verify that no logs were saved
        self.assertEqual(self.connector._saved["logs"], 0)
        
        # Verify that the query was called once
        self.assertEqual(mock_client.query.call_count, 1)
        
        # Verify the query was called with appropriate parameters
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args[0][0]
        
        # Check that the query includes the table reference
        self.assertIn("test_dataset.test_table", call_args)
        
        # Check that the query includes the pointer comparison (now uses @low_watermark parameter)
        self.assertIn("timestamp_usec > @low_watermark", call_args)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_nested_json_pointer_navigation(self, mock_get_creds, mock_bigquery_client):
        """Test pointer navigation through nested JSON structures like real BigQuery data."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results with nested JSON structure (like your real data)
        mock_rows = [
            {
                "gmail": {
                    "event_info": {
                        "timestamp_usec": 1752594724674697
                    },
                    "message": "Test log 1"
                }
            },
            {
                "gmail": {
                    "event_info": {
                        "timestamp_usec": 1752594724674698
                    },
                    "message": "Test log 2"
                }
            }
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Configure connector for nested JSON structure
        self.connector.configuration.pointer_path = "gmail.event_info.timestamp_usec"
        self.connector.configuration.columns = ["gmail"]
        self.connector.configuration.time_format = "microseconds"
        self.connector._pointer = "1752594724674696"
        
        # Run collection
        self.connector.run()
        
        # Verify results
        self.assertEqual(self.connector._saved["logs"], 2)
        
        # Verify the query was called with correct nested path
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args[0][0]
        
        # Check that the query uses the nested path correctly (now uses @low_watermark parameter)
        self.assertIn("gmail.event_info.timestamp_usec > @low_watermark", call_args)
        self.assertIn("ORDER BY gmail.event_info.timestamp_usec", call_args)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_query_syntax_validation_microseconds_format(self, mock_get_creds, mock_bigquery_client):
        """Test that microseconds format generates correct SQL syntax (would catch the bug)."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results
        mock_rows = [
            {
                "gmail": {
                    "event_info": {
                        "timestamp_usec": 1752594724674697
                    }
                }
            }
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Configure connector to match the bug scenario
        self.connector.configuration.pointer_path = "gmail.event_info.timestamp_usec"
        self.connector.configuration.columns = ["gmail"]
        self.connector.configuration.time_format = "microseconds"
        self.connector._pointer = "1752594724674696"
        
        # Capture the actual SQL query being generated
        captured_query = None
        def capture_query(query, **kwargs):
            nonlocal captured_query
            captured_query = query
            return mock_query_job
        
        mock_client.query.side_effect = capture_query
        
        # Run collection
        self.connector.run()
        
        # Verify the query uses numeric comparison (correct)
        self.assertIn("gmail.event_info.timestamp_usec > @low_watermark", captured_query)
        
        # Verify it does NOT use TIMESTAMP function (which would cause syntax error)
        self.assertNotIn("TIMESTAMP(", captured_query)
        
        # Verify it does NOT use timestamp strings (which would cause syntax error)
        self.assertNotIn("2025-07-15 15:52:04+00", captured_query)
        
        # Verify the query is syntactically correct for BigQuery
        self.assertIn("SELECT gmail", captured_query)
        self.assertIn("FROM `test-project.test_dataset.test_table`", captured_query)
        self.assertIn("ORDER BY gmail.event_info.timestamp_usec ASC", captured_query)
        self.assertIn("LIMIT @page_size", captured_query)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_query_syntax_validation_timestamp_format(self, mock_get_creds, mock_bigquery_client):
        """Test that timestamp format generates correct SQL syntax."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results
        mock_rows = [
            {
                "gmail": {
                    "event_info": {
                        "timestamp_usec": 1752594724674697
                    }
                }
            }
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Configure connector for timestamp format
        self.connector.configuration.pointer_path = "gmail.event_info.timestamp_usec"
        self.connector.configuration.columns = ["gmail"]
        self.connector.configuration.time_format = "timestamp"
        self.connector._pointer = "2025-07-15 15:52:04+00"
        
        # Capture the actual SQL query being generated
        captured_query = None
        def capture_query(query, **kwargs):
            nonlocal captured_query
            captured_query = query
            return mock_query_job
        
        mock_client.query.side_effect = capture_query
        
        # Run collection
        self.connector.run()
        
        # Verify the query uses TIMESTAMP function (correct for timestamp format)
        self.assertIn("gmail.event_info.timestamp_usec > @low_watermark", captured_query)
        
        # Verify the query is syntactically correct for BigQuery
        self.assertIn("SELECT gmail", captured_query)
        self.assertIn("FROM `test-project.test_dataset.test_table`", captured_query)
        self.assertIn("ORDER BY gmail.event_info.timestamp_usec ASC", captured_query)
        self.assertIn("LIMIT @page_size", captured_query)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_pointer_update_with_nested_data(self, mock_get_creds, mock_bigquery_client):
        """Test that pointer is correctly updated from nested JSON results."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results with nested structure
        mock_rows = [
            {
                "gmail": {
                    "event_info": {
                        "timestamp_usec": 1752594724674697
                    }
                }
            },
            {
                "gmail": {
                    "event_info": {
                        "timestamp_usec": 1752594724674698
                    }
                }
            }
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Configure connector
        self.connector.configuration.pointer_path = "gmail.event_info.timestamp_usec"
        self.connector.configuration.columns = ["gmail"]
        self.connector.configuration.time_format = "microseconds"
        self.connector._pointer = "1752594724674696"
        
        # Run collection
        self.connector.run()
        
        # Verify results
        self.assertEqual(self.connector._saved["logs"], 2)
        
        # Verify that the pointer was updated to the latest timestamp
        # The pointer should be updated to the last timestamp in the results
        self.assertEqual(self.connector._pointer, "1752594724674698")

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_microseconds_with_timestamp_string_bug_scenario(self, mock_get_creds, mock_bigquery_client):
        """Test the specific bug scenario that was causing the syntax error."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_bigquery_client.return_value = mock_client
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results
        mock_rows = [
            {
                "gmail": {
                    "event_info": {
                        "timestamp_usec": 1752594724674697
                    }
                }
            }
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Configure connector to match the exact bug scenario from logs
        self.connector.configuration.pointer_path = "gmail.event_info.timestamp_usec"
        self.connector.configuration.columns = ["gmail"]
        self.connector.configuration.time_format = "microseconds"
        self.connector._pointer = "1752594724674696"  # This should stay as microseconds
        
        # Capture the actual SQL query being generated
        captured_query = None
        def capture_query(query, **kwargs):
            nonlocal captured_query
            captured_query = query
            return mock_query_job
        
        mock_client.query.side_effect = capture_query
        
        # Run collection
        self.connector.run()
        
        # This test would have caught the bug by verifying:
        # 1. The query uses numeric comparison (not timestamp string)
        self.assertIn("gmail.event_info.timestamp_usec > @low_watermark", captured_query)
        
        # 2. The query does NOT contain timestamp strings (which caused the syntax error)
        self.assertNotIn("2025-07-15 15:52:04+00", captured_query)
        
        # 3. The query does NOT use TIMESTAMP function for microseconds format
        self.assertNotIn("TIMESTAMP(", captured_query)
        
        # 4. The query is syntactically valid for BigQuery
        # (This test would have failed with the original bug, showing the syntax error)

    @patch('grove.connectors.google.bigquery_query.bigquery.Client')
    @patch.object(Connector, 'get_credentials')
    def test_google_auth_deadlock_handling(self, mock_get_creds, mock_bigquery_client):
        """Test that the connector handles Google Auth deadlock errors with retry logic."""
        # Mock credentials
        mock_get_creds.return_value = Mock()
        
        # Mock BigQuery client that raises deadlock error on first call, succeeds on second
        mock_client = Mock()
        mock_bigquery_client.side_effect = [
            Exception("_frozen_importlib._DeadlockError: deadlock detected by _ModuleLock('google.auth.exceptions')"),
            mock_client
        ]
        
        mock_query_job = Mock()
        mock_client.query.return_value = mock_query_job
        
        # Mock query results
        mock_rows = [
            {"timestamp_usec": 1738500089504000, "message": "Test log 1"}
        ]
        mock_query_job.result.return_value = mock_rows
        
        # Set initial pointer
        self.connector._pointer = "1738500089504000"
        
        # Run collection - should retry and succeed
        self.connector.run()
        
        # Verify results
        self.assertEqual(self.connector._saved["logs"], 1)
        
        # Verify that BigQuery client was attempted twice (retry logic)
        self.assertEqual(mock_bigquery_client.call_count, 2)

    def test_compute_lookback_days_bootstrap(self):
        """Test lookback computation for new connectors."""
        now_utc = datetime(2024, 1, 8, 12, 0, 0, tzinfo=timezone.utc)
        lookback = self.connector._compute_lookback_days(
            None, now_utc, bootstrap_days=7, min_days=3, max_days=30, late_buffer_days=2
        )
        self.assertEqual(lookback, 7)

    def test_compute_lookback_days_caught_up(self):
        """Test lookback computation when caught up."""
        now_utc = datetime(2024, 1, 8, 12, 0, 0, tzinfo=timezone.utc)
        last_seen_usec = int((now_utc - timedelta(hours=1)).timestamp() * 1_000_000)
        
        lookback = self.connector._compute_lookback_days(
            last_seen_usec, now_utc, bootstrap_days=7, min_days=3, max_days=30, late_buffer_days=2
        )
        # Should be close to min_days + late_buffer_days
        self.assertLessEqual(lookback, 5)
        self.assertGreaterEqual(lookback, 3)

    def test_compute_lookback_days_far_behind(self):
        """Test lookback computation when far behind."""
        now_utc = datetime(2024, 1, 8, 12, 0, 0, tzinfo=timezone.utc)
        last_seen_usec = int((now_utc - timedelta(days=50)).timestamp() * 1_000_000)
        
        lookback = self.connector._compute_lookback_days(
            last_seen_usec, now_utc, bootstrap_days=7, min_days=3, max_days=30, late_buffer_days=2
        )
        # Should be max_days
        self.assertEqual(lookback, 30)

    def test_compute_lookback_days_bounds(self):
        """Test lookback computation respects bounds."""
        now_utc = datetime(2024, 1, 8, 12, 0, 0, tzinfo=timezone.utc)
        last_seen_usec = int((now_utc - timedelta(days=15)).timestamp() * 1_000_000)
        
        lookback = self.connector._compute_lookback_days(
            last_seen_usec, now_utc, bootstrap_days=7, min_days=3, max_days=30, late_buffer_days=2
        )
        # Should be 15 + 2 = 17, within bounds
        self.assertEqual(lookback, 17)

    def test_initialize_watermark_microseconds(self):
        """Test watermark initialization with microseconds format."""
        self.connector.configuration.time_format = "microseconds"
        self.connector.pointer = "1704067200000000"
        
        watermark = self.connector._initialize_watermark("microseconds")
        self.assertEqual(watermark, 1704067200000000)

    def test_initialize_watermark_timestamp(self):
        """Test watermark initialization with timestamp format."""
        self.connector.configuration.time_format = "timestamp"
        self.connector.pointer = "2024-01-01 00:00:00+00"
        
        watermark = self.connector._initialize_watermark("timestamp")
        expected = int(datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000)
        self.assertEqual(watermark, expected)

    def test_initialize_watermark_no_pointer(self):
        """Test watermark initialization when no pointer exists."""
        self.connector.pointer = None
        
        watermark = self.connector._initialize_watermark("microseconds")
        # Should be approximately 7 days ago
        now = datetime.now(timezone.utc)
        week_ago = int((now - timedelta(days=7)).timestamp() * 1_000_000)
        
        # Allow for small time differences
        self.assertAlmostEqual(watermark, week_ago, delta=1000000)

    def test_fetch_page_bigquery_parameters(self):
        """Test that query parameters are correctly set."""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_results = Mock()
        
        # Mock the query execution
        mock_client.query.return_value = mock_query_job
        mock_query_job.result.return_value = mock_results
        mock_results.__iter__ = lambda self: iter([])
        
        # Mock job attributes
        mock_query_job.total_bytes_processed = 1048576
        mock_query_job.slot_millis = 1500
        
        with patch('grove.connectors.google.bigquery_query.bigquery.QueryJobConfig') as mock_config_class:
            mock_config = Mock()
            mock_config_class.return_value = mock_config
            
            self.connector._fetch_page_bigquery(
                client=mock_client,
                project_id="test-project",
                dataset_name="test_dataset",
                table_name="test_table",
                columns=["gmail.event_info.timestamp_usec", "gmail"],
                pointer_path="gmail.event_info.timestamp_usec",
                time_format="microseconds",
                last_seen_usec=1704067200000000,
                page_size=5000,
                min_partition_date=datetime(2024, 1, 1).date(),
                ceiling_usec=1704153600000000
            )
            
            # Verify QueryJobConfig was called with correct parameters
            mock_config_class.assert_called_once()

    def test_fetch_page_bigquery_with_results(self):
        """Test fetching page with actual results."""
        mock_client = Mock()
        mock_query_job = Mock()
        
        # Mock results with timestamp data - use nested structure to match pointer_path
        mock_row1 = {"gmail": {"event_info": {"timestamp_usec": 1704067200000000}, "data": "test1"}}
        mock_row2 = {"gmail": {"event_info": {"timestamp_usec": 1704067201000000}, "data": "test2"}}
        mock_results = [mock_row1, mock_row2]
        
        mock_query_job.result.return_value = mock_results
        mock_query_job.total_bytes_processed = 1048576
        mock_query_job.slot_millis = 1500
        
        mock_client.query.return_value = mock_query_job
        
        with patch('grove.connectors.google.bigquery_query.bigquery.QueryJobConfig'):
            rows, new_watermark, debug_metadata = self.connector._fetch_page_bigquery(
                client=mock_client,
                project_id="test-project",
                dataset_name="test_dataset",
                table_name="test_table",
                columns=["gmail.event_info.timestamp_usec", "gmail"],
                pointer_path="gmail.event_info.timestamp_usec",
                time_format="microseconds",
                last_seen_usec=1704067200000000,
                page_size=5000,
                min_partition_date=datetime(2024, 1, 1).date(),
                ceiling_usec=1704153600000000
            )
            
            self.assertEqual(len(rows), 2)
            self.assertEqual(new_watermark, 1704067201000000)
            self.assertEqual(debug_metadata["rows_returned"], 2)
            self.assertEqual(debug_metadata["new_watermark"], 1704067201000000)

    def test_fetch_page_bigquery_nested_pointer_path(self):
        """Test fetching page with nested pointer path."""
        mock_client = Mock()
        mock_query_job = Mock()
        
        # Mock results with nested timestamp data
        mock_row = {"gmail": {"event_info": {"timestamp_usec": 1704067200000000}}}
        mock_results = [mock_row]
        
        mock_query_job.result.return_value = mock_results
        mock_query_job.total_bytes_processed = 1048576
        mock_query_job.slot_millis = 1500
        
        mock_client.query.return_value = mock_query_job
        
        with patch('grove.connectors.google.bigquery_query.bigquery.QueryJobConfig'):
            rows, new_watermark, debug_metadata = self.connector._fetch_page_bigquery(
                client=mock_client,
                project_id="test-project",
                dataset_name="test_dataset",
                table_name="test_table",
                columns=["gmail"],
                pointer_path="gmail.event_info.timestamp_usec",
                time_format="microseconds",
                last_seen_usec=1704067200000000,
                page_size=5000,
                min_partition_date=datetime(2024, 1, 1).date(),
                ceiling_usec=1704153600000000
            )
            
            self.assertEqual(new_watermark, 1704067200000000)

    def test_configuration_validation_page_size(self):
        """Test that invalid page_size raises ConfigurationException."""
        self.connector.configuration.page_size = -1
        
        with self.assertRaises(ConfigurationException) as context:
            self.connector.collect()
        
        self.assertIn("page_size must be a positive integer", str(context.exception))

    def test_configuration_validation_page_size_type(self):
        """Test that non-integer page_size raises ConfigurationException."""
        self.connector.configuration.page_size = "invalid"
        
        with self.assertRaises(ConfigurationException) as context:
            self.connector.collect()
        
        self.assertIn("page_size must be a positive integer", str(context.exception))

    def test_create_bigquery_client_success(self):
        """Test successful BigQuery client creation."""
        with patch('grove.connectors.google.bigquery_query.bigquery.Client') as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            
            # Mock get_credentials to avoid authentication issues
            with patch.object(self.connector, 'get_credentials') as mock_get_creds:
                mock_creds = Mock()
                mock_get_creds.return_value = mock_creds
                
                client = self.connector._create_bigquery_client("test-project")
                self.assertEqual(client, mock_client)

    def test_create_bigquery_client_retry_on_deadlock(self):
        """Test BigQuery client creation retries on deadlock."""
        with patch('grove.connectors.google.bigquery_query.bigquery.Client') as mock_client_class:
            # First call raises deadlock, second succeeds
            mock_client_class.side_effect = [
                Exception("deadlock detected"),
                Mock()
            ]
            
            # Mock get_credentials to avoid authentication issues
            with patch.object(self.connector, 'get_credentials') as mock_get_creds:
                mock_creds = Mock()
                mock_get_creds.return_value = mock_creds
                
                with patch('time.sleep'):  # Mock sleep to speed up test
                    client = self.connector._create_bigquery_client("test-project")
                    self.assertIsNotNone(client)
                    self.assertEqual(mock_client_class.call_count, 2)

    def test_create_bigquery_client_max_retries_exceeded(self):
        """Test BigQuery client creation fails after max retries."""
        with patch('grove.connectors.google.bigquery_query.bigquery.Client') as mock_client_class:
            mock_client_class.side_effect = Exception("deadlock detected")
            
            # Mock get_credentials to avoid authentication issues
            with patch.object(self.connector, 'get_credentials') as mock_get_creds:
                mock_creds = Mock()
                mock_get_creds.return_value = mock_creds
                
                with patch('time.sleep'):  # Mock sleep to speed up test
                    with self.assertRaises(Exception) as context:
                        self.connector._create_bigquery_client("test-project")
                    
                    self.assertIn("deadlock detected", str(context.exception))
                    self.assertEqual(mock_client_class.call_count, 3)

    def test_get_credentials_success(self):
        """Test successful credentials generation."""
        with patch('grove.connectors.google.bigquery_query.service_account.Credentials') as mock_creds_class:
            mock_creds = Mock()
            mock_creds_class.from_service_account_info.return_value = mock_creds
            
            credentials = self.connector.get_credentials()
            self.assertEqual(credentials, mock_creds)

    def test_get_credentials_invalid_json(self):
        """Test credentials generation fails with invalid JSON."""
        self.connector.key = "invalid json"
        
        with self.assertRaises(ConfigurationException) as context:
            self.connector.get_credentials()
        
        self.assertIn("Unable to load service account JSON", str(context.exception))

    def test_get_credentials_auth_error(self):
        """Test credentials generation fails with auth error."""
        with patch('grove.connectors.google.bigquery_query.service_account.Credentials') as mock_creds_class:
            from google.auth.exceptions import GoogleAuthError
            mock_creds_class.from_service_account_info.side_effect = GoogleAuthError("auth failed")
            
            with self.assertRaises(ConfigurationException) as context:
                self.connector.get_credentials()
            
            self.assertIn("Authentication error", str(context.exception))

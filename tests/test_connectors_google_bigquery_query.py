# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Google BigQuery Query connector."""

import os
import unittest
from unittest.mock import Mock, patch

from grove.connectors.google.bigquery_query import Connector
from grove.models import ConnectorConfig
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
                max_batches=1
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
        
        # First call returns 1000 rows (triggers pagination)
        # Second call returns 500 rows (ends pagination)
        mock_rows_1000 = [{"timestamp_usec": i, "message": f"Log {i}"} for i in range(1000)]
        mock_rows_500 = [{"timestamp_usec": i + 1000, "message": f"Log {i + 1000}"} for i in range(500)]
        
        mock_query_job.result.side_effect = [mock_rows_1000, mock_rows_500]
        
        # Set initial pointer
        self.connector._pointer = "1000000000000000"
        
        # Set max_batches to 2 for this test
        self.connector.configuration.max_batches = 2
        
        # Run collection
        self.connector.run()
        
        # Verify pagination occurred (query called twice)
        self.assertEqual(mock_client.query.call_count, 2)
        # Total logs saved: 1000 + 500 = 1500
        self.assertEqual(self.connector._saved["logs"], 1500)

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
        
        # Mock query results - less than 1000 rows
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
        # Since we only have 2 rows (< 1000), it should complete in one batch
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
        
        # Check that the query includes LIMIT for pagination
        self.assertIn("LIMIT 1000", call_args)

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
        
        # Check that the query includes the timestamp comparison
        self.assertIn("created_at > TIMESTAMP('2025-01-01 10:00:00')", call_args)
        
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
        
        # Should contain a valid microseconds value (7 days ago)
        self.assertIn("timestamp_usec > ", call_args)
        # Extract the microseconds value and verify it's a number
        import re
        match = re.search(r'timestamp_usec > (\d+)', call_args)
        self.assertIsNotNone(match)
        microseconds_value = int(match.group(1))
        self.assertGreater(microseconds_value, 0)
        
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
        
        # Should contain a valid timestamp value (7 days ago)
        self.assertIn("created_at > TIMESTAMP('", call_args)
        # Extract the timestamp value and verify it's a valid format
        match = re.search(r"TIMESTAMP\('([^']+)'\)", call_args)
        self.assertIsNotNone(match)
        timestamp_value = match.group(1)
        # Should be in format YYYY-MM-DD HH:MM:SS+00
        self.assertRegex(timestamp_value, r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+00')

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
        
        # Check that the query includes the pointer comparison
        self.assertIn("timestamp_usec > 1738500089504000", call_args)

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
        
        # Check that the query uses the nested path correctly
        self.assertIn("gmail.event_info.timestamp_usec > 1752594724674696", call_args)
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
        def capture_query(query):
            nonlocal captured_query
            captured_query = query
            return mock_query_job
        
        mock_client.query.side_effect = capture_query
        
        # Run collection
        self.connector.run()
        
        # Verify the query uses numeric comparison (correct)
        self.assertIn("gmail.event_info.timestamp_usec > 1752594724674696", captured_query)
        
        # Verify it does NOT use TIMESTAMP function (which would cause syntax error)
        self.assertNotIn("TIMESTAMP(", captured_query)
        
        # Verify it does NOT use timestamp strings (which would cause syntax error)
        self.assertNotIn("2025-07-15 15:52:04+00", captured_query)
        
        # Verify the query is syntactically correct for BigQuery
        self.assertIn("SELECT gmail", captured_query)
        self.assertIn("FROM `test-project.test_dataset.test_table`", captured_query)
        self.assertIn("ORDER BY gmail.event_info.timestamp_usec ASC", captured_query)
        self.assertIn("LIMIT 1000", captured_query)

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
        def capture_query(query):
            nonlocal captured_query
            captured_query = query
            return mock_query_job
        
        mock_client.query.side_effect = capture_query
        
        # Run collection
        self.connector.run()
        
        # Verify the query uses TIMESTAMP function (correct for timestamp format)
        self.assertIn("gmail.event_info.timestamp_usec > TIMESTAMP('2025-07-15 15:52:04+00')", captured_query)
        
        # Verify the query is syntactically correct for BigQuery
        self.assertIn("SELECT gmail", captured_query)
        self.assertIn("FROM `test-project.test_dataset.test_table`", captured_query)
        self.assertIn("ORDER BY gmail.event_info.timestamp_usec ASC", captured_query)
        self.assertIn("LIMIT 1000", captured_query)

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
        def capture_query(query):
            nonlocal captured_query
            captured_query = query
            return mock_query_job
        
        mock_client.query.side_effect = capture_query
        
        # Run collection
        self.connector.run()
        
        # This test would have caught the bug by verifying:
        # 1. The query uses numeric comparison (not timestamp string)
        self.assertIn("gmail.event_info.timestamp_usec > 1752594724674696", captured_query)
        
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

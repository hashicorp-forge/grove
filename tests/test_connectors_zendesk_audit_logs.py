"""Implements unit tests for the Zendesk Audit Logs connector."""

import os
import re
import unittest
from unittest.mock import patch, Mock
from datetime import datetime, timezone, timedelta

import responses

from grove.connectors.zendesk.audit_logs import AuditLogsConnector
from grove.models import ConnectorConfig
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RateLimitException,
    RequestFailedException,
)
from tests import mocks


class ZendeskAuditLogsTestCase(unittest.TestCase):
    """Implements unit tests for the Zendesk Audit Logs connector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        
        # Base configuration for audit logs connector
        self.base_config = {
            "subdomain": "test-company",
            "identity": "test@example.com",
            "key": "test_api_token",
            "connector": "zendesk_audit_logs",
            "name": "test-zendesk-audit-logs",
        }
        
        self.connector = AuditLogsConnector(
            config=ConnectorConfig(**self.base_config),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        # Patch save to track logs
        self.connector._saved = {"logs": 0}
        def fake_save(logs):
            self.connector._saved["logs"] += len(logs)
            print(f"Fake save called with {len(logs)} logs. Total saved: {self.connector._saved['logs']}")
        self.connector.save = fake_save

    def test_subdomain_property(self):
        """Test subdomain property retrieval."""
        self.assertEqual(self.connector.subdomain, "test-company")

    def test_missing_subdomain_raises_exception(self):
        """Test that missing subdomain raises ConfigurationException."""
        config = self.base_config.copy()
        del config["subdomain"]
        
        with self.assertRaises(ConfigurationException):
            AuditLogsConnector(
                config=ConnectorConfig(**config),
                context={"runtime": "test_harness", "runtime_id": "NA"},
            )

    def test_batch_size_default(self):
        """Test that batch_size defaults to 100."""
        self.assertEqual(self.connector.batch_size, 100)

    def test_batch_size_configured(self):
        """Test that batch_size can be configured."""
        config = self.base_config.copy()
        config["batch_size"] = 50
        
        connector = AuditLogsConnector(
            config=ConnectorConfig(**config),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(connector.batch_size, 50)

    def test_delay_default(self):
        """Test that delay defaults to 0."""
        self.assertEqual(self.connector.delay, 0)

    def test_delay_configured(self):
        """Test that delay can be configured."""
        config = self.base_config.copy()
        config["delay"] = 10
        
        connector = AuditLogsConnector(
            config=ConnectorConfig(**config),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(connector.delay, 10)

    def test_enforce_rate_limit_default(self):
        """Test that enforce_rate_limit defaults to False."""
        self.assertFalse(self.connector.enforce_rate_limit)

    def test_enforce_rate_limit_configured(self):
        """Test that enforce_rate_limit can be configured."""
        config = self.base_config.copy()
        config["enforce_rate_limit"] = True
        
        connector = AuditLogsConnector(
            config=ConnectorConfig(**config),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertTrue(connector.enforce_rate_limit)

    @responses.activate
    def test_get_audit_logs_success(self):
        """Test successful audit logs retrieval."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/audit_logs.json",
            json={
                "audit_logs": [
                    {"id": 1, "created_at": "2023-01-01T00:00:00Z"},
                    {"id": 2, "created_at": "2023-01-01T00:01:00Z"}
                ],
                "meta": {
                    "has_more": False
                }
            },
            status=200,
        )
        
        logs = self.connector.client.get_audit_logs()
        
        self.assertEqual(len(logs["audit_logs"]), 2)
        self.assertEqual(logs["audit_logs"][0]["id"], 1)
        self.assertEqual(logs["audit_logs"][1]["id"], 2)

    @responses.activate
    def test_get_audit_logs_with_cursor(self):
        """Test audit logs retrieval with cursor pagination."""
        # First page response
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/audit_logs.json",
            json={
                "audit_logs": [
                    {"id": 1, "created_at": "2023-01-01T00:00:00Z"}
                ],
                "meta": {
                    "has_more": True,
                    "after_cursor": "cursor123"
                }
            },
            status=200,
        )
        
        # Second page response
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/audit_logs.json",
            json={
                "audit_logs": [
                    {"id": 2, "created_at": "2023-01-01T00:01:00Z"}
                ],
                "meta": {
                    "has_more": False
                }
            },
            status=200,
        )
        
        # Get first page
        logs = self.connector.client.get_audit_logs()
        self.assertEqual(len(logs["audit_logs"]), 1)
        self.assertTrue(logs["meta"]["has_more"])
        
        # Get second page
        logs = self.connector.client.get_audit_logs(cursor=logs["meta"]["after_cursor"])
        self.assertEqual(len(logs["audit_logs"]), 1)
        self.assertFalse(logs["meta"]["has_more"])

    @responses.activate
    def test_get_audit_logs_with_date_range(self):
        """Test audit logs retrieval with date range filtering."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/audit_logs.json",
            json={
                "audit_logs": [
                    {"id": 1, "created_at": "2023-01-01T00:00:00Z"}
                ],
                "meta": {
                    "has_more": False
                }
            },
            status=200,
        )
        
        logs = self.connector.client.get_audit_logs(
            start_date="2023-01-01T00:00:00Z",
            end_date="2023-01-02T00:00:00Z"
        )
        
        self.assertEqual(len(logs["audit_logs"]), 1)
        self.assertEqual(logs["audit_logs"][0]["id"], 1)

    @responses.activate
    def test_collect_with_no_previous_pointer(self):
        """Test collection with no previous pointer."""
        # Use timestamps within the last 7 days
        now = datetime.now(timezone.utc)
        log1_time = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        log2_time = (now - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
        responses.add(
            responses.GET,
            re.compile(r"https://test-company\.zendesk\.com/api/v2/audit_logs\.json.*"),
            json={
                "audit_logs": [
                    {"id": 1, "created_at": log1_time},
                    {"id": 2, "created_at": log2_time}
                ],
                "meta": {
                    "has_more": False
                }
            },
            status=200,
        )

        # Patch _get_time_range to bypass cache lookup
        def fake_get_time_range():
            start_time = now - timedelta(days=7)
            end_time = now
            return start_time, end_time
        self.connector._get_time_range = fake_get_time_range

        # Mock the pointer property to simulate no previous pointer
        def fake_pointer_getter(self):
            raise NotFoundException("No value found in cache")
        self.connector.pointer = property(fake_pointer_getter)

        # Mock the save method to track logs
        saved_logs = []
        def fake_save(logs):
            saved_logs.extend(logs)
        self.connector.save = fake_save

        self.connector.collect()

        # Verify logs were saved
        self.assertEqual(len(saved_logs), 2)
        self.assertEqual(saved_logs[0]["id"], 1)
        self.assertEqual(saved_logs[1]["id"], 2)

    def _load_fixture(self, filename):
        """Load a test fixture file."""
        import json
        with open(os.path.join(self.dir, "fixtures", "zendesk", filename), "r") as f:
            return json.load(f) 
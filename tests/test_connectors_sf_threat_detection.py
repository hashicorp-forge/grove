# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements integration tests for the SalesForce Threat Detection collector."""

import os
import re
import unittest
from datetime import timedelta
from unittest.mock import patch

import responses

from grove.connectors.sf.threat_detection import Connector
from grove.exceptions import RequestFailedException, ConfigurationException
from grove.models import ConnectorConfig
from tests import mocks


class SFThreatDetectionTestCase(unittest.TestCase):
    """Implements integration tests for the SalesForce Threat Detection collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="Someuser",
                key="token",
                name="test",
                connector="test",
                token="12345",
                operation="ApiAnomaly",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_collect_fail_on_server_error(self):
        """Ensure server errors raise an appropriate exception."""
        # Ensure authentication succeeds (POST to SF).
        login_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/login.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            body=login_response,
            content_type="application/xml",
        )

        # Ensure first query returns an HTTP 500 (GET to SF)
        query_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/error.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=500,
            body=query_response,
            content_type="application/xml",
        )

        with self.assertRaises(RequestFailedException):
            self.connector.collect()

    def test_oauth_configuration_detection(self):
        """Test OAuth 2.0 configuration detection."""
        oauth_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="testuser@example.com",
                instance_url="https://testorg.my.salesforce.com",
                name="test-oauth",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertTrue(oauth_connector._is_oauth_configured())
        self.assertFalse(oauth_connector._is_legacy_configured())
        self.assertEqual(oauth_connector.client_id, "test_client_id")
        self.assertEqual(oauth_connector.client_secret, "test_client_secret")
        self.assertEqual(oauth_connector.instance_url, "https://testorg.my.salesforce.com")

    def test_legacy_configuration_detection(self):
        """Test legacy username/password configuration detection."""
        legacy_connector = Connector(
            config=ConnectorConfig(
                key="test_password",
                identity="testuser@example.com",
                token="test_security_token",
                name="test-legacy",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertFalse(legacy_connector._is_oauth_configured())
        self.assertTrue(legacy_connector._is_legacy_configured())
        self.assertEqual(legacy_connector.key, "test_password")
        self.assertEqual(legacy_connector.identity, "testuser@example.com")
        self.assertEqual(legacy_connector.token, "test_security_token")

    def test_mixed_configuration_oauth_preferred(self):
        """Test that OAuth is preferred when both authentication methods are configured."""
        mixed_connector = Connector(
            config=ConnectorConfig(
                key="test_password",
                identity="testuser@example.com",
                token="test_security_token",
                client_id="test_client_id",
                client_secret="test_client_secret",
                instance_url="https://testorg.my.salesforce.com",
                name="test-mixed",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertTrue(mixed_connector._is_oauth_configured())
        self.assertTrue(mixed_connector._is_legacy_configured())

    def test_invalid_configuration_no_credentials(self):
        """Test that missing credentials raise appropriate exception."""
        invalid_connector = Connector(
            config=ConnectorConfig(
                identity="testuser@example.com",
                name="test-invalid",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertFalse(invalid_connector._is_oauth_configured())
        self.assertFalse(invalid_connector._is_legacy_configured())
        
        with self.assertRaises(ConfigurationException) as context:
            invalid_connector.collect()
        self.assertIn("Either OAuth 2.0 credentials", str(context.exception))

    @responses.activate
    def test_oauth_authentication_success(self):
        """Test successful OAuth 2.0 authentication."""
        oauth_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="testuser@example.com",
                instance_url="https://testorg.my.salesforce.com",
                name="test-oauth",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        # Mock OAuth token response
        oauth_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/threat_detection/oauth_token.json"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            "https://testorg.my.salesforce.com/services/oauth2/token",
            status=200,
            body=oauth_response,
            content_type="application/json",
        )
        
        # Mock Shield availability check (test query)
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*ApiAnomalyEventStore.*LIMIT"),
            status=200,
            body='{"totalSize": 0, "done": true, "records": []}',
            content_type="application/json",
        )

        # Mock ApiAnomalyEventStore query response
        query_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/threat_detection/credential_stuffing_direct.json"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*"),
            status=200,
            body=query_response,
            content_type="application/json",
        )
        
        # Test OAuth authentication
        access_token, instance_url = oauth_connector.get_oauth_access_token()
        self.assertEqual(access_token, "XXXXXXXXXXXXXXX!TOKENTOKENTOKENTOKENTOKENTOKENTOKENTOKENTOKENTOKENTOKENTOKENTOKENTOKENTOKEN")
        self.assertEqual(instance_url, "https://example.my.salesforce.com")

    @responses.activate
    def test_oauth_authentication_failure(self):
        """Test OAuth 2.0 authentication failure."""
        oauth_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="testuser@example.com",
                instance_url="https://testorg.my.salesforce.com",
                name="test-oauth",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        # Mock OAuth token failure
        responses.add(
            responses.POST,
            "https://test.my.salesforce.com/services/oauth2/token",
            status=400,
            body='{"error": "invalid_client", "error_description": "Invalid client credentials"}',
            content_type="application/json",
        )
        
        with self.assertRaises(RequestFailedException) as context:
            oauth_connector.get_oauth_access_token()
        self.assertIn("Unable to authenticate with Salesforce using OAuth 2.0", str(context.exception))

    def test_oauth_missing_client_id(self):
        """Test OAuth authentication with missing client_id."""
        oauth_connector = Connector(
            config=ConnectorConfig(
                client_secret="test_client_secret",
                identity="testuser@example.com",
                instance_url="https://testorg.my.salesforce.com",
                name="test-oauth",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        with self.assertRaises(ConfigurationException) as context:
            oauth_connector.get_oauth_access_token()
        self.assertIn("client_id is required for OAuth 2.0 authentication", str(context.exception))

    def test_oauth_missing_client_secret(self):
        """Test OAuth authentication with missing client_secret."""
        oauth_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                identity="testuser@example.com",
                instance_url="https://testorg.my.salesforce.com",
                name="test-oauth",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        with self.assertRaises(ConfigurationException) as context:
            oauth_connector.get_oauth_access_token()
        self.assertIn("client_secret is required for OAuth 2.0 authentication", str(context.exception))

    def test_legacy_missing_password(self):
        """Test legacy authentication with missing password."""
        legacy_connector = Connector(
            config=ConnectorConfig(
                identity="testuser@example.com",
                token="test_security_token",
                name="test-legacy",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        with self.assertRaises(ConfigurationException) as context:
            legacy_connector.get_legacy_credentials()
        self.assertIn("key (password) is required for legacy authentication", str(context.exception))

    def test_legacy_missing_username(self):
        """Test legacy authentication with missing username."""
        # Create a connector with identity but test the method directly
        legacy_connector = Connector(
            config=ConnectorConfig(
                key="test_password",
                identity="testuser@example.com",  # Required by ConnectorConfig
                token="test_security_token",
                name="test-legacy",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        # Manually set identity to None to test the validation
        legacy_connector.identity = None
        
        with self.assertRaises(ConfigurationException) as context:
            legacy_connector.get_legacy_credentials()
        self.assertIn("identity (username) is required for legacy authentication", str(context.exception))

    def test_legacy_missing_token(self):
        """Test legacy authentication with missing security token."""
        legacy_connector = Connector(
            config=ConnectorConfig(
                key="test_password",
                identity="testuser@example.com",
                name="test-legacy",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        with self.assertRaises(ConfigurationException) as context:
            legacy_connector.get_legacy_credentials()
        self.assertIn("token (security token) is required for legacy authentication", str(context.exception))

    def test_oauth_url_selection(self):
        """Test OAuth URL selection based on instance URL."""
        # Test custom domain URL selection
        custom_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="testuser@example.com",
                instance_url="https://testorg.my.salesforce.com",
                name="test-custom",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        # Should use instance-specific OAuth endpoint
        self.assertEqual(custom_connector._get_oauth_token_url(), "https://testorg.my.salesforce.com/services/oauth2/token")
        
        # Test sandbox URL selection
        sandbox_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="testuser@example.com",
                instance_url="https://test.salesforce.com",
                name="test-sandbox",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(sandbox_connector._get_oauth_token_url(), "https://test.salesforce.com/services/oauth2/token")
        
        # Test production URL selection
        prod_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="testuser@example.com",
                instance_url="https://testcompany.lightning.force.com",
                name="test-production",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(prod_connector._get_oauth_token_url(), "https://testcompany.lightning.force.com/services/oauth2/token")
        
        # Test default to production when no instance URL
        no_instance_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="testuser@example.com",
                name="test-no-instance",
                connector="sf_threat_detection",
                operation="ApiAnomaly",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(no_instance_connector._get_oauth_token_url(), "https://login.salesforce.com/services/oauth2/token")

    def test_invalid_operation(self):
        """Test that invalid operations raise appropriate exception."""
        invalid_connector = Connector(
            config=ConnectorConfig(
                key="test_password",
                identity="testuser@example.com",
                token="test_security_token",
                name="test-invalid-op",
                connector="sf_threat_detection",
                operation="InvalidOperation",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        with self.assertRaises(ConfigurationException) as context:
            invalid_connector.collect()
        self.assertIn("Operation must be one of", str(context.exception))

    @responses.activate
    def test_collect_credential_stuffing_events(self):
        """Test collection of credential stuffing events with specific fields."""
        credential_connector = Connector(
            config=ConnectorConfig(
                identity="Someuser",
                key="token",
                name="test-credential",
                connector="test",
                token="12345",
                operation="ApiAnomaly",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

        # Ensure authentication succeeds (POST to SF).
        login_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/login.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            body=login_response,
            content_type="application/xml",
        )

        # Mock Shield availability check (test query)
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*ApiAnomalyEventStore.*LIMIT"),
            status=200,
            body='{"totalSize": 0, "done": true, "records": []}',
            content_type="application/json",
        )

        # Mock ApiAnomalyEventStore query response
        query_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/threat_detection/credential_stuffing_direct.json"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*"),
            status=200,
            body=query_response,
            content_type="application/json",
        )

        # Should not raise an exception
        credential_connector.collect()
        self.assertEqual(credential_connector._saved["logs"], 1)

    @responses.activate
    def test_collect_report_anomaly_events(self):
        """Test collection of report anomaly events with SecurityEventData field."""
        report_connector = Connector(
            config=ConnectorConfig(
                identity="Someuser",
                key="token",
                name="test-report",
                connector="test",
                token="12345",
                operation="ReportAnomaly",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

        # Ensure authentication succeeds (POST to SF).
        login_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/login.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            body=login_response,
            content_type="application/xml",
        )

        # Mock Shield availability check (test query)
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*ReportAnomalyEventStore.*LIMIT"),
            status=200,
            body='{"totalSize": 0, "done": true, "records": []}',
            content_type="application/json",
        )

        # Mock ReportAnomalyEventStore query response
        query_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/threat_detection/report_anomaly_direct.json"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*"),
            status=200,
            body=query_response,
            content_type="application/json",
        )

        # Should not raise an exception
        report_connector.collect()
        self.assertEqual(report_connector._saved["logs"], 1)

    def test_backfill_configuration(self):
        """Test backfill mode with start_date configuration."""
        backfill_connector = Connector(
            config=ConnectorConfig(
                identity="Someuser",
                key="token",
                name="test-backfill",
                connector="test",
                token="12345",
                operation="ApiAnomaly",
                start_date="2025-01-01T00:00:00.000Z",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        
        self.assertEqual(backfill_connector.start_date, "2025-01-01T00:00:00.000Z")
        self.assertEqual(backfill_connector.max_retries, 3)
        self.assertEqual(backfill_connector.retry_delay, 1)

    def test_rate_limiting_configuration(self):
        """Test rate limiting configuration options."""
        rate_limit_connector = Connector(
            config=ConnectorConfig(
                identity="Someuser",
                key="token",
                name="test-rate-limit",
                connector="test",
                token="12345",
                operation="ApiAnomaly",
                max_retries=5,
                retry_delay=2,
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        
        self.assertEqual(rate_limit_connector.max_retries, 5)
        self.assertEqual(rate_limit_connector.retry_delay, 2)

    @responses.activate
    def test_shield_availability_check_failure(self):
        """Test Shield availability check failure."""
        # Ensure authentication succeeds (POST to SF).
        login_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/login.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            body=login_response,
            content_type="application/xml",
        )

        # Mock Shield availability check failure (INVALID_TYPE)
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*ApiAnomalyEventStore.*LIMIT"),
            status=400,
            body='[{"errorCode": "INVALID_TYPE", "message": "sObject type \'ApiAnomalyEventStore\' is not supported"}]',
            content_type="application/json",
        )

        with self.assertRaises(RequestFailedException) as context:
            self.connector.collect()
        self.assertIn("Salesforce Shield Event Monitoring is not available", str(context.exception))

    @responses.activate
    def test_invalid_type_error_handling(self):
        """Test INVALID_TYPE error handling with helpful message."""
        # Ensure authentication succeeds (POST to SF).
        login_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/login.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            body=login_response,
            content_type="application/xml",
        )

        # Mock Shield availability check success
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*ApiAnomalyEventStore.*LIMIT"),
            status=200,
            body='{"totalSize": 0, "done": true, "records": []}',
            content_type="application/json",
        )

        # Mock query failure with INVALID_TYPE
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*"),
            status=400,
            body='[{"errorCode": "INVALID_TYPE", "message": "sObject type \'ApiAnomalyEventStore\' is not supported"}]',
            content_type="application/json",
        )

        with self.assertRaises(RequestFailedException) as context:
            self.connector.collect()
        self.assertIn("Salesforce Shield licensing or permissions issue detected", str(context.exception))
        self.assertIn("View Threat Detection Events", str(context.exception))

    @responses.activate
    def test_invalid_field_error_handling(self):
        """Test INVALID_FIELD error handling with helpful message."""
        # Ensure authentication succeeds (POST to SF).
        login_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/login.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            body=login_response,
            content_type="application/xml",
        )

        # Mock Shield availability check success
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*ApiAnomalyEventStore.*LIMIT"),
            status=200,
            body='{"totalSize": 0, "done": true, "records": []}',
            content_type="application/json",
        )

        # Mock query failure with INVALID_FIELD
        responses.add(
            responses.GET,
            re.compile(r"https://.*/services/data/v51.0/query.*"),
            status=400,
            body='[{"errorCode": "INVALID_FIELD", "message": "No such column \'Score\' on entity \'ApiAnomalyEventStore\'"}]',
            content_type="application/json",
        )

        with self.assertRaises(RequestFailedException) as context:
            self.connector.collect()
        self.assertIn("Field-level permissions issue detected", str(context.exception))
        self.assertIn("field-level access", str(context.exception))

    def test_timestamp_parsing(self):
        """Test Salesforce timestamp parsing with Z format."""
        from grove.connectors.sf.threat_detection import parse_salesforce_timestamp
        from datetime import datetime, timezone
        
        # Test Z format
        z_timestamp = "2025-09-16T12:34:56.000Z"
        result = parse_salesforce_timestamp(z_timestamp)
        expected = datetime(2025, 9, 16, 12, 34, 56, 0, timezone.utc)
        self.assertEqual(result, expected)
        
        # Test timezone offset format
        offset_timestamp = "2025-09-16T12:34:56.000+00:00"
        result = parse_salesforce_timestamp(offset_timestamp)
        self.assertEqual(result, expected)
        
        # Test different timezone
        est_timestamp = "2025-09-16T12:34:56.000-05:00"
        result = parse_salesforce_timestamp(est_timestamp)
        expected_est = datetime(2025, 9, 16, 12, 34, 56, 0, timezone(timedelta(hours=-5)))
        self.assertEqual(result, expected_est)

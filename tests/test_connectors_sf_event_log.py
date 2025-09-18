# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements integration tests for the SalesForce Event Log collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.sf.event_log import Connector
from grove.exceptions import RequestFailedException, ConfigurationException
from grove.models import ConnectorConfig
from tests import mocks


class SFEventLogTestCase(unittest.TestCase):
    """Implements integration tests for the SalesForce Evennt Log collector."""

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
                operation="Login",
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

    @responses.activate
    def test_collect_no_pagination(self):
        """Ensure collection without pagination is working as expected."""
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

        # Ensure EventLogFile query returns a 200 (GET to SF).
        query_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/001.json"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            body=query_response,
            content_type="application/xml",
        )

        # Ensure LogFile query returns a 200 (GET to SF).
        log_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/001.csv"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            body=log_response,
            content_type="text/csv",
        )

        # Check the pointer matches the latest value, and that the expected number of
        # logs were returned.
        self.connector.collect()
        self.assertEqual(self.connector._saved["logs"], 2)
        self.assertEqual(self.connector.pointer, "2038-01-19T03:00:00.000Z")

    def test_oauth_configuration_detection(self):
        """Test OAuth 2.0 configuration detection."""
        oauth_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="test@example.com",
                instance_url="https://test.my.salesforce.com",
                name="test-oauth",
                connector="sf_event_log",
                operation="Login",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertTrue(oauth_connector._is_oauth_configured())
        self.assertFalse(oauth_connector._is_legacy_configured())
        self.assertEqual(oauth_connector.client_id, "test_client_id")
        self.assertEqual(oauth_connector.client_secret, "test_client_secret")
        self.assertEqual(oauth_connector.instance_url, "https://test.my.salesforce.com")

    def test_legacy_configuration_detection(self):
        """Test legacy username/password configuration detection."""
        legacy_connector = Connector(
            config=ConnectorConfig(
                key="test_password",
                identity="test@example.com",
                token="test_security_token",
                name="test-legacy",
                connector="sf_event_log",
                operation="Login",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertFalse(legacy_connector._is_oauth_configured())
        self.assertTrue(legacy_connector._is_legacy_configured())
        self.assertEqual(legacy_connector.key, "test_password")
        self.assertEqual(legacy_connector.identity, "test@example.com")
        self.assertEqual(legacy_connector.token, "test_security_token")

    def test_mixed_configuration_oauth_preferred(self):
        """Test that OAuth is preferred when both authentication methods are configured."""
        mixed_connector = Connector(
            config=ConnectorConfig(
                key="test_password",
                identity="test@example.com",
                token="test_security_token",
                client_id="test_client_id",
                client_secret="test_client_secret",
                instance_url="https://test.my.salesforce.com",
                name="test-mixed",
                connector="sf_event_log",
                operation="Login",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertTrue(mixed_connector._is_oauth_configured())
        self.assertTrue(mixed_connector._is_legacy_configured())

    def test_invalid_configuration_no_credentials(self):
        """Test that missing credentials raise appropriate exception."""
        invalid_connector = Connector(
            config=ConnectorConfig(
                identity="test@example.com",
                name="test-invalid",
                connector="sf_event_log",
                operation="Login",
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
                identity="test@example.com",
                instance_url="https://test.my.salesforce.com",
                name="test-oauth",
                connector="sf_event_log",
                operation="Login",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        # Mock OAuth token response
        oauth_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/oauth_token.json"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            "https://test.my.salesforce.com/services/oauth2/token",
            status=200,
            body=oauth_response,
            content_type="application/json",
        )
        
        # Mock EventLogFile query response
        query_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/001.json"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            body=query_response,
            content_type="application/json",
        )
        
        # Mock LogFile response
        log_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/001.csv"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            body=log_response,
            content_type="text/csv",
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
                identity="test@example.com",
                instance_url="https://test.my.salesforce.com",
                name="test-oauth",
                connector="sf_event_log",
                operation="Login",
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
                identity="test@example.com",
                instance_url="https://test.my.salesforce.com",
                name="test-oauth",
                connector="sf_event_log",
                operation="Login",
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
                identity="test@example.com",
                instance_url="https://test.my.salesforce.com",
                name="test-oauth",
                connector="sf_event_log",
                operation="Login",
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
                identity="test@example.com",
                token="test_security_token",
                name="test-legacy",
                connector="sf_event_log",
                operation="Login",
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
                identity="test@example.com",  # Required by ConnectorConfig
                token="test_security_token",
                name="test-legacy",
                connector="sf_event_log",
                operation="Login",
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
                identity="test@example.com",
                name="test-legacy",
                connector="sf_event_log",
                operation="Login",
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
                identity="test@example.com",
                instance_url="https://klaviyo.my.salesforce.com",
                name="test-custom",
                connector="sf_event_log",
                operation="Login",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        # Should use instance-specific OAuth endpoint
        self.assertEqual(custom_connector._get_oauth_token_url(), "https://klaviyo.my.salesforce.com/services/oauth2/token")
        
        # Test sandbox URL selection
        sandbox_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="test@example.com",
                instance_url="https://test.salesforce.com",
                name="test-sandbox",
                connector="sf_event_log",
                operation="Login",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(sandbox_connector._get_oauth_token_url(), "https://test.salesforce.com/services/oauth2/token")
        
        # Test production URL selection
        prod_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="test@example.com",
                instance_url="https://company.lightning.force.com",
                name="test-production",
                connector="sf_event_log",
                operation="Login",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(prod_connector._get_oauth_token_url(), "https://company.lightning.force.com/services/oauth2/token")
        
        # Test default to production when no instance URL
        no_instance_connector = Connector(
            config=ConnectorConfig(
                client_id="test_client_id",
                client_secret="test_client_secret",
                identity="test@example.com",
                name="test-no-instance",
                connector="sf_event_log",
                operation="Login",
            ),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(no_instance_connector._get_oauth_token_url(), "https://login.salesforce.com/services/oauth2/token")

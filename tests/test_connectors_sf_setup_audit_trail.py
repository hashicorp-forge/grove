# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Tests for the Salesforce Setup Audit Trail connector."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from simple_salesforce.exceptions import SalesforceError

from grove.connectors.sf.setup_audit_trail import Connector
from grove.exceptions import ConfigurationException, RequestFailedException


class TestSFSetupAuditTrailConnector:
    """Tests for the Salesforce Setup Audit Trail connector."""

    @pytest.fixture
    def connector(self):
        """Create a connector instance for testing."""
        config = MagicMock()
        config.client_id = "test_client_id"
        config.client_secret = "test_client_secret"
        config.instance_url = "https://testorg.my.salesforce.com"
        config.start_date = "2024-01-01T00:00:00.000Z"
        config.max_retries = 3
        config.retry_delay = 1
        config.reference.return_value = "test-connector"
        # Ensure legacy credentials are not present
        config.key = None
        config.identity = None
        config.token = None

        context = {
            "runtime": "test",
            "runtime_id": "test-123",
            "runtime_host": "test-host-12345",
        }

        return Connector(config, context)

    @pytest.fixture
    def legacy_connector(self):
        """Create a legacy connector instance for testing."""
        config = MagicMock()
        config.key = "test_password"
        config.identity = "testuser@example.com"
        config.token = "test_security_token"
        config.start_date = "2024-01-01T00:00:00.000Z"
        config.max_retries = 3
        config.retry_delay = 1
        config.reference.return_value = "test-connector"
        # Ensure OAuth credentials are not present
        config.client_id = None
        config.client_secret = None
        config.instance_url = "https://testorg.my.salesforce.com"

        context = {
            "runtime": "test",
            "runtime_id": "test-123",
            "runtime_host": "test-host-12345",
        }

        return Connector(config, context)

    def test_oauth_configuration_detection(self, connector):
        """Test OAuth configuration detection."""
        assert connector._is_oauth_configured() is True
        assert connector._is_legacy_configured() is False

    def test_legacy_configuration_detection(self, legacy_connector):
        """Test legacy configuration detection."""
        assert legacy_connector._is_oauth_configured() is False
        assert legacy_connector._is_legacy_configured() is True

    def test_no_configuration_raises_exception(self):
        """Test that missing configuration raises an exception."""
        config = MagicMock()
        config.client_id = None
        config.client_secret = None
        config.key = None
        config.identity = None
        config.token = None

        context = {
            "runtime": "test",
            "runtime_id": "test-123",
            "runtime_host": "test-host-12345",
        }

        connector = Connector(config, context)

        with pytest.raises(ConfigurationException):
            connector.collect()

    @patch("grove.connectors.sf.setup_audit_trail.requests.session")
    def test_oauth_authentication_success(self, mock_session, connector):
        """Test successful OAuth authentication."""
        # Mock the OAuth response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "test_access_token",
            "instance_url": "https://testorg.my.salesforce.com",
        }
        mock_response.raise_for_status.return_value = None
        mock_session.return_value.post.return_value = mock_response

        access_token, instance_url = connector.get_oauth_access_token()

        assert access_token == "test_access_token"
        assert instance_url == "https://testorg.my.salesforce.com"

    @patch("grove.connectors.sf.setup_audit_trail.requests.session")
    def test_oauth_authentication_failure(self, mock_session, connector):
        """Test OAuth authentication failure."""
        # Mock the OAuth response with error
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "error": "invalid_client",
            "error_description": "Invalid client credentials",
        }
        mock_response.raise_for_status.side_effect = Exception("HTTP 400")
        mock_session.return_value.post.return_value = mock_response

        with pytest.raises(Exception):
            connector.get_oauth_access_token()

    @patch("grove.connectors.sf.setup_audit_trail.SalesforceLogin")
    def test_legacy_authentication_success(self, mock_salesforce_login, legacy_connector):
        """Test successful legacy authentication."""
        mock_salesforce_login.return_value = ("test_session_id", "https://testorg.my.salesforce.com")

        session_id, instance_url = legacy_connector.get_legacy_credentials()

        assert session_id == "test_session_id"
        assert instance_url == "https://testorg.my.salesforce.com"

    @patch("grove.connectors.sf.setup_audit_trail.SalesforceLogin")
    def test_legacy_authentication_failure(self, mock_salesforce_login, legacy_connector):
        """Test legacy authentication failure."""
        mock_salesforce_login.side_effect = SalesforceError("Invalid credentials", 400, "test", "test")

        with pytest.raises(RequestFailedException):
            legacy_connector.get_legacy_credentials()

    @patch("grove.connectors.sf.setup_audit_trail.Salesforce")
    @patch("grove.connectors.sf.setup_audit_trail.requests.session")
    def test_collect_oauth_success(self, mock_session, mock_salesforce, connector):
        """Test successful data collection with OAuth."""
        # Mock OAuth response
        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {
            "access_token": "test_access_token",
            "instance_url": "https://testorg.my.salesforce.com",
        }
        mock_oauth_response.raise_for_status.return_value = None
        mock_session.return_value.post.return_value = mock_oauth_response

        # Mock Salesforce client and query results
        mock_client = MagicMock()
        mock_client.query_all.return_value = {
            "totalSize": 2,
            "records": [
                {
                    "Id": "test_id_1",
                    "Action": "create",
                    "Section": "User",
                    "CreatedDate": "2024-01-15T10:30:00.000Z",
                    "Display": "Created user testuser@example.com",
                    "CreatedBy": {"Username": "admin@example.com"},
                    "DelegateUser": None,
                },
                {
                    "Id": "test_id_2",
                    "Action": "modify",
                    "Section": "Profile",
                    "CreatedDate": "2024-01-15T11:00:00.000Z",
                    "Display": "Modified profile System Administrator",
                    "CreatedBy": {"Username": "admin@example.com"},
                    "DelegateUser": "delegate@example.com",
                },
            ],
        }
        mock_salesforce.return_value = mock_client

        # Mock the save method
        connector.save = MagicMock()

        # Mock pointer handling
        connector.pointer = "2024-01-01T00:00:00.000Z"

        connector.collect()

        # Verify the query was called
        mock_client.query_all.assert_called_once()
        query_args = mock_client.query_all.call_args[0][0]
        assert "SetupAuditTrail" in query_args
        assert "CreatedDate >= 2024-01-01T00:00:00.000000Z" in query_args

        # Verify save was called with the expected entries
        connector.save.assert_called_once()
        saved_entries = connector.save.call_args[0][0]
        assert len(saved_entries) == 2
        assert saved_entries[0]["_grove_operation"] == "SetupAuditTrail"
        assert saved_entries[0]["Action"] == "create"
        assert saved_entries[0]["CreatedByUsername"] == "admin@example.com"

    @patch("grove.connectors.sf.setup_audit_trail.Salesforce")
    @patch("grove.connectors.sf.setup_audit_trail.SalesforceLogin")
    def test_collect_legacy_success(self, mock_salesforce_login, mock_salesforce, legacy_connector):
        """Test successful data collection with legacy authentication."""
        # Mock legacy authentication
        mock_salesforce_login.return_value = ("test_session_id", "https://testorg.my.salesforce.com")

        # Mock Salesforce client and query results
        mock_client = MagicMock()
        mock_client.query_all.return_value = {
            "totalSize": 1,
            "records": [
                {
                    "Id": "test_id_1",
                    "Action": "create",
                    "Section": "User",
                    "CreatedDate": "2024-01-15T10:30:00.000Z",
                    "Display": "Created user testuser@example.com",
                    "CreatedBy": {"Username": "admin@example.com"},
                    "DelegateUser": None,
                },
            ],
        }
        mock_salesforce.return_value = mock_client

        # Mock the save method
        legacy_connector.save = MagicMock()

        # Mock pointer handling
        legacy_connector.pointer = "2024-01-01T00:00:00.000Z"

        legacy_connector.collect()

        # Verify the query was called
        mock_client.query_all.assert_called_once()
        query_args = mock_client.query_all.call_args[0][0]
        assert "SetupAuditTrail" in query_args

        # Verify save was called
        legacy_connector.save.assert_called_once()

    @patch("grove.connectors.sf.setup_audit_trail.Salesforce")
    @patch("grove.connectors.sf.setup_audit_trail.requests.session")
    def test_collect_invalid_type_error(self, mock_session, mock_salesforce, connector):
        """Test handling of INVALID_TYPE error (permissions issue)."""
        # Mock OAuth response
        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {
            "access_token": "test_access_token",
            "instance_url": "https://testorg.my.salesforce.com",
        }
        mock_oauth_response.raise_for_status.return_value = None
        mock_session.return_value.post.return_value = mock_oauth_response

        # Mock Salesforce client with INVALID_TYPE error
        mock_client = MagicMock()
        mock_client.query_all.side_effect = SalesforceError("INVALID_TYPE: sObject type 'SetupAuditTrail' is not supported", 400, "test", "test")
        mock_salesforce.return_value = mock_client

        # Mock pointer handling
        connector.pointer = "2024-01-01T00:00:00.000Z"

        with pytest.raises(RequestFailedException) as exc_info:
            connector.collect()

        assert "permissions issue detected" in str(exc_info.value)
        assert "View Setup and Configuration" in str(exc_info.value)

    @patch("grove.connectors.sf.setup_audit_trail.Salesforce")
    @patch("grove.connectors.sf.setup_audit_trail.requests.session")
    def test_collect_rate_limit_retry(self, mock_session, mock_salesforce, connector):
        """Test rate limit handling with retry logic."""
        # Mock OAuth response
        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {
            "access_token": "test_access_token",
            "instance_url": "https://testorg.my.salesforce.com",
        }
        mock_oauth_response.raise_for_status.return_value = None
        mock_session.return_value.post.return_value = mock_oauth_response

        # Mock Salesforce client with rate limit error, then success
        mock_client = MagicMock()
        rate_limit_error = SalesforceError("REQUEST_LIMIT_EXCEEDED", 400, "test", "test")
        rate_limit_error.error_code = "REQUEST_LIMIT_EXCEEDED"
        
        mock_client.query_all.side_effect = [
            rate_limit_error,
            {
                "totalSize": 0,
                "records": [],
            },
        ]
        mock_salesforce.return_value = mock_client

        # Mock pointer handling
        connector.pointer = "2024-01-01T00:00:00.000Z"

        # Mock the save method
        connector.save = MagicMock()

        connector.collect()

        # Verify the query was called twice (retry)
        assert mock_client.query_all.call_count == 2

    @patch("grove.connectors.sf.setup_audit_trail.Salesforce")
    @patch("grove.connectors.sf.setup_audit_trail.requests.session")
    def test_collect_no_records(self, mock_session, mock_salesforce, connector):
        """Test handling when no records are returned."""
        # Mock OAuth response
        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {
            "access_token": "test_access_token",
            "instance_url": "https://testorg.my.salesforce.com",
        }
        mock_oauth_response.raise_for_status.return_value = None
        mock_session.return_value.post.return_value = mock_oauth_response

        # Mock Salesforce client with no records
        mock_client = MagicMock()
        mock_client.query_all.return_value = {
            "totalSize": 0,
            "records": [],
        }
        mock_salesforce.return_value = mock_client

        # Mock pointer handling
        connector.pointer = "2024-01-01T00:00:00.000Z"

        # Mock the save method
        connector.save = MagicMock()

        connector.collect()

        # Verify save was not called
        connector.save.assert_not_called()

    def test_oauth_url_selection(self, connector):
        """Test OAuth URL selection based on instance URL."""
        # Test with instance URL
        oauth_url = connector._get_oauth_token_url()
        assert oauth_url == "https://testorg.my.salesforce.com/services/oauth2/token"

        # Test without instance URL (fallback) - create a new connector with no instance_url
        config = MagicMock()
        config.client_id = "test_client_id"
        config.client_secret = "test_client_secret"
        config.instance_url = None
        config.start_date = "2024-01-01T00:00:00.000Z"
        config.max_retries = 3
        config.retry_delay = 1
        config.reference.return_value = "test-connector"
        config.key = None
        config.identity = None
        config.token = None

        context = {
            "runtime": "test",
            "runtime_id": "test-123",
            "runtime_host": "test-host-12345",
        }

        no_instance_connector = Connector(config, context)
        oauth_url = no_instance_connector._get_oauth_token_url()
        assert oauth_url == "https://login.salesforce.com/services/oauth2/token"

    def test_parse_salesforce_timestamp(self):
        """Test Salesforce timestamp parsing."""
        from grove.connectors.sf.setup_audit_trail import parse_salesforce_timestamp

        # Test with Z suffix
        timestamp = parse_salesforce_timestamp("2024-01-15T10:30:00.000Z")
        assert timestamp.year == 2024
        assert timestamp.month == 1
        assert timestamp.day == 15
        assert timestamp.hour == 10
        assert timestamp.minute == 30

        # Test with timezone offset
        timestamp = parse_salesforce_timestamp("2024-01-15T10:30:00.000+00:00")
        assert timestamp.year == 2024
        assert timestamp.month == 1
        assert timestamp.day == 15
        assert timestamp.hour == 10
        assert timestamp.minute == 30

    @patch("grove.connectors.sf.setup_audit_trail.Salesforce")
    @patch("grove.connectors.sf.setup_audit_trail.requests.session")
    def test_pointer_update(self, mock_session, mock_salesforce, connector):
        """Test pointer update after successful collection."""
        # Mock OAuth response
        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {
            "access_token": "test_access_token",
            "instance_url": "https://testorg.my.salesforce.com",
        }
        mock_oauth_response.raise_for_status.return_value = None
        mock_session.return_value.post.return_value = mock_oauth_response

        # Mock Salesforce client with records
        mock_client = MagicMock()
        mock_client.query_all.return_value = {
            "totalSize": 1,
            "records": [
                {
                    "Id": "test_id_1",
                    "Action": "create",
                    "Section": "User",
                    "CreatedDate": "2024-01-15T10:30:00.000Z",
                    "Display": "Created user testuser@example.com",
                    "CreatedBy": {"Username": "admin@example.com"},
                    "DelegateUser": None,
                },
            ],
        }
        mock_salesforce.return_value = mock_client

        # Mock the save method
        connector.save = MagicMock()

        # Mock pointer handling
        connector.pointer = "2024-01-01T00:00:00.000Z"

        connector.collect()

        # Verify pointer was updated
        assert connector.pointer == "2024-01-15T10:30:00.000Z"

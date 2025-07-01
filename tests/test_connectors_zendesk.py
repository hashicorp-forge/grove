# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Zendesk connectors."""

import os
import re
import unittest
from unittest.mock import patch, Mock
from datetime import datetime, timezone, timedelta

import responses

from grove.connectors.zendesk.tickets import Connector as TicketsConnector
from grove.connectors.zendesk.search_api import Connector as SearchConnector
from grove.connectors.zendesk.api import ZendeskClient
from grove.models import ConnectorConfig
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RateLimitException,
    RequestFailedException,
)
from tests import mocks


class ZendeskTicketsTestCase(unittest.TestCase):
    """Implements unit tests for the Zendesk Tickets connector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        
        # Base configuration for tickets connector
        self.base_config = {
            "subdomain": "test-company",
            "identity": "test@example.com",
            "key": "test_api_token",
            "connector": "zendesk_tickets",
            "name": "test-zendesk-tickets",
        }
        
        self.connector = TicketsConnector(
            config=ConnectorConfig(**self.base_config),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    def test_subdomain_property(self):
        """Test subdomain property retrieval."""
        self.assertEqual(self.connector.subdomain, "test-company")

    def test_missing_subdomain_raises_exception(self):
        """Test that missing subdomain raises ConfigurationException at init."""
        config = self.base_config.copy()
        del config["subdomain"]
        with self.assertRaises(ConfigurationException):
            TicketsConnector(
                config=ConnectorConfig(**config),
                context={"runtime": "test_harness", "runtime_id": "NA"},
            )

    def test_include_comments_default(self):
        """Test that include_comments defaults to True."""
        self.assertTrue(self.connector.include_comments)

    def test_include_comments_configured(self):
        """Test that include_comments can be configured."""
        config = self.base_config.copy()
        config["include_comments"] = False
        
        connector = TicketsConnector(
            config=ConnectorConfig(**config),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertFalse(connector.include_comments)

    def test_delay_minutes_default(self):
        """Test that delay_minutes defaults to 5."""
        self.assertEqual(self.connector.delay_minutes, 5)

    def test_delay_minutes_configured(self):
        """Test that delay_minutes can be configured."""
        config = self.base_config.copy()
        config["delay_minutes"] = 10
        
        connector = TicketsConnector(
            config=ConnectorConfig(**config),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(connector.delay_minutes, 10)

    def test_batch_size_default(self):
        """Test that batch_size defaults to 50."""
        self.assertEqual(self.connector.batch_size, 50)

    def test_batch_size_configured(self):
        """Test that batch_size can be configured."""
        config = self.base_config.copy()
        config["batch_size"] = 25
        
        connector = TicketsConnector(
            config=ConnectorConfig(**config),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(connector.batch_size, 25)

    @responses.activate
    def test_make_request_success(self):
        """Test successful API request using ZendeskClient."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/test",
            json={"test": "data"},
            status=200,
        )
        client = ZendeskClient(
            subdomain="test-company",
            identity="test@example.com",
            api_token="test_api_token"
        )
        result = client._make_request("test")
        self.assertEqual(result, {"test": "data"})

    @responses.activate
    def test_make_request_rate_limit(self):
        """Test rate limit handling using ZendeskClient."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/test",
            status=429,
            headers={"Retry-After": "60"},
        )
        client = ZendeskClient(
            subdomain="test-company",
            identity="test@example.com",
            api_token="test_api_token"
        )
        with self.assertRaises(RateLimitException) as cm:
            client._make_request("test")
        self.assertIn("Rate limited", str(cm.exception))

    @responses.activate
    def test_make_request_error(self):
        """Test API request error handling using ZendeskClient."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/test",
            json={"error": "Not found"},
            status=404,
        )
        client = ZendeskClient(
            subdomain="test-company",
            identity="test@example.com",
            api_token="test_api_token"
        )
        with self.assertRaises(RequestFailedException) as cm:
            client._make_request("test")
        self.assertIn("Request failed with status 404", str(cm.exception))

    @responses.activate
    def test_get_tickets_since(self):
        """Test getting tickets since a specific time."""
        # Mock incremental API response
        responses.add(
            responses.GET,
            re.compile(r"https://test-company\.zendesk\.com/api/v2/incremental/tickets/cursor\.json.*"),
            json=self._load_fixture("incremental_tickets.json"),
            status=200,
        )
        
        # Mock end of stream response
        responses.add(
            responses.GET,
            re.compile(r"https://test-company\.zendesk\.com/api/v2/incremental/tickets/cursor\.json.*cursor.*"),
            json=self._load_fixture("incremental_tickets_end.json"),
            status=200,
        )
        
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        tickets = self.connector._get_tickets_since(start_time)
        
        self.assertEqual(len(tickets), 2)
        self.assertEqual(tickets[0]["id"], 1)
        self.assertEqual(tickets[1]["id"], 2)

    @responses.activate
    def test_get_ticket_comments(self):
        """Test getting comments for a ticket."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/tickets/1/comments.json",
            json=self._load_fixture("ticket_comments.json"),
            status=200,
        )
        
        comments = self.connector._get_ticket_comments(1)
        
        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[0]["id"], 101)
        self.assertEqual(comments[1]["id"], 102)
        self.assertEqual(len(comments[1]["attachments"]), 1)

    def test_filter_closed_tickets(self):
        """Test filtering tickets to only closed ones."""
        tickets = [
            {"id": 1, "status": "closed"},
            {"id": 2, "status": "open"},
            {"id": 3, "status": "solved"},
            {"id": 4, "status": "pending"},
        ]
        
        closed_tickets = self.connector._filter_closed_tickets(tickets)
        
        self.assertEqual(len(closed_tickets), 2)
        self.assertEqual({t["id"] for t in closed_tickets}, {1, 3})

    @responses.activate
    def test_enrich_tickets_with_comments(self):
        """Test enriching tickets with comments."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/tickets/1/comments.json",
            json=self._load_fixture("ticket_comments.json"),
            status=200,
        )
        
        tickets = [{"id": 1, "subject": "Test ticket"}]
        enriched = self.connector._enrich_tickets_with_comments(tickets)
        
        self.assertEqual(len(enriched), 1)
        self.assertIn("comments", enriched[0])
        self.assertEqual(len(enriched[0]["comments"]), 2)

    @responses.activate
    def test_collect_with_no_previous_pointer(self):
        """Test collection with no previous pointer."""
        # Mock incremental API response
        responses.add(
            responses.GET,
            re.compile(r"https://test-company\.zendesk\.com/api/v2/incremental/tickets/cursor\.json.*"),
            json=self._load_fixture("incremental_tickets.json"),
            status=200,
        )
        
        # Mock end of stream response  
        responses.add(
            responses.GET,
            re.compile(r"https://test-company\.zendesk\.com/api/v2/incremental/tickets/cursor\.json.*cursor.*"),
            json=self._load_fixture("incremental_tickets_end.json"),
            status=200,
        )
        
        # Mock ticket comments
        responses.add(
            responses.GET,
            re.compile(r"https://test-company\.zendesk\.com/api/v2/tickets/\d+/comments\.json.*"),
            json=self._load_fixture("ticket_comments.json"),
            status=200,
        )
        
        self.connector.collect()
        
        # Verify tickets were saved
        self.assertGreater(self.connector._saved.get("logs", 0), 0)

    def _load_fixture(self, filename):
        """Load a test fixture file."""
        import json
        with open(os.path.join(self.dir, "fixtures", "zendesk", filename), "r") as f:
            return json.load(f)


class ZendeskSearchTestCase(unittest.TestCase):
    """Implements unit tests for the Zendesk Search API connector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        
        # Base configuration for search connector
        self.base_config = {
            "subdomain": "test-company",
            "identity": "test@example.com",
            "key": "test_api_token",
            "connector": "zendesk_search",
            "name": "test-zendesk-search",
        }
        
        self.connector = SearchConnector(
            config=ConnectorConfig(**self.base_config),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    def test_ticket_status_default(self):
        """Test that ticket_status defaults to 'closed'."""
        self.assertEqual(self.connector.ticket_status, "closed")

    def test_ticket_status_configured(self):
        """Test that ticket_status can be configured."""
        config = self.base_config.copy()
        config["status"] = "pending"
        
        connector = SearchConnector(
            config=ConnectorConfig(**config),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        self.assertEqual(connector.ticket_status, "pending")

    def test_include_comments_default(self):
        """Test that include_comments defaults to True."""
        self.assertTrue(self.connector.include_comments)

    def test_delay_minutes_default(self):
        """Test that delay_minutes defaults to 5."""
        self.assertEqual(self.connector.delay_minutes, 5)

    @responses.activate
    def test_search_tickets(self):
        """Test searching for tickets using ZendeskClient."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/search.json",
            match=[responses.matchers.query_param_matcher({
                "query": "type:ticket status:closed",
                "page": "1",
                "per_page": "100",
                "sort_by": "updated_at",
                "sort_order": "asc"
            })],
            json=self._load_fixture("search_results.json"),
            status=200,
        )
        client = ZendeskClient(
            subdomain="test-company",
            identity="test@example.com",
            api_token="test_api_token"
        )
        query = "type:ticket status:closed"
        tickets = client.search_tickets(query)
        self.assertEqual(len(tickets), 2)
        self.assertEqual(tickets[0]["id"], 1)
        self.assertEqual(tickets[1]["id"], 2)

    @responses.activate
    def test_search_tickets_no_results(self):
        """Test searching with no results using ZendeskClient."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/search.json",
            match=[responses.matchers.query_param_matcher({
                "query": "type:ticket status:nonexistent",
                "page": "1",
                "per_page": "100",
                "sort_by": "updated_at",
                "sort_order": "asc"
            })],
            json=self._load_fixture("search_no_results.json"),
            status=200,
        )
        client = ZendeskClient(
            subdomain="test-company",
            identity="test@example.com",
            api_token="test_api_token"
        )
        query = "type:ticket status:nonexistent"
        tickets = client.search_tickets(query)
        self.assertEqual(len(tickets), 0)

    def test_build_search_query(self):
        """Test building search query."""
        start_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        query = self.connector._build_search_query(start_time)
        
        expected = "type:ticket status:closed updated>=2023-01-01"
        self.assertEqual(query, expected)

    def test_build_search_query_custom_status(self):
        """Test building search query with custom status."""
        config = self.base_config.copy()
        config["status"] = "pending"
        
        connector = SearchConnector(
            config=ConnectorConfig(**config),
            context={"runtime": "test_harness", "runtime_id": "NA"},
        )
        
        start_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        query = connector._build_search_query(start_time)
        
        expected = "type:ticket status:pending updated>=2023-01-01"
        self.assertEqual(query, expected)

    @responses.activate
    def test_collect_with_results(self):
        """Test collection with search results."""
        # Mock search API response
        responses.add(
            responses.GET,
            re.compile(r"https://test-company\.zendesk\.com/api/v2/search\.json.*"),
            json=self._load_fixture("search_results.json"),
            status=200,
        )
        
        # Mock ticket comments
        responses.add(
            responses.GET,
            re.compile(r"https://test-company\.zendesk\.com/api/v2/tickets/\d+/comments\.json.*"),
            json=self._load_fixture("ticket_comments.json"),
            status=200,
        )
        
        self.connector.collect()
        
        # Verify tickets were saved
        self.assertGreater(self.connector._saved.get("logs", 0), 0)

    @responses.activate
    def test_collect_no_results(self):
        """Test collection with no search results."""
        responses.add(
            responses.GET,
            re.compile(r"https://test-company\.zendesk\.com/api/v2/search\.json.*"),
            json=self._load_fixture("search_no_results.json"),
            status=200,
        )
        
        self.connector.collect()
        
        # Verify no tickets were saved
        self.assertEqual(self.connector._saved.get("logs", 0), 0)

    def _load_fixture(self, filename):
        """Load a test fixture file."""
        import json
        with open(os.path.join(self.dir, "fixtures", "zendesk", filename), "r") as f:
            return json.load(f)


class ZendeskClientTestCase(unittest.TestCase):
    """Implements unit tests for the Zendesk API client."""

    def setUp(self):
        """Set up test client."""
        self.client = ZendeskClient(
            subdomain="test-company",
            identity="test@example.com",
            api_token="test_token"
        )

    def test_client_initialization(self):
        """Test client initialization."""
        self.assertEqual(self.client.base_url, "https://test-company.zendesk.com/api/v2/")
        self.assertEqual(self.client.auth, ("test@example.com/token", "test_token"))

    @responses.activate
    def test_search_tickets(self):
        """Test search tickets method."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/search.json",
            json={
                "results": [
                    {"id": 1, "result_type": "ticket"},
                    {"id": 2, "result_type": "user"},  # Should be filtered out
                    {"id": 3, "result_type": "ticket"}
                ],
                "next_page": None
            },
            status=200
        )
        
        tickets = self.client.search_tickets("type:ticket")
        
        # Should only return ticket results
        self.assertEqual(len(tickets), 2)
        self.assertEqual(tickets[0]["id"], 1)
        self.assertEqual(tickets[1]["id"], 3)

    @responses.activate
    def test_get_ticket_comments(self):
        """Test get ticket comments method."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/tickets/1/comments.json",
            json={
                "comments": [
                    {"id": 101, "body": "Comment 1"},
                    {"id": 102, "body": "Comment 2"}
                ],
                "next_page": None
            },
            status=200
        )
        
        comments = self.client.get_ticket_comments(1)
        
        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[0]["id"], 101)
        self.assertEqual(comments[1]["id"], 102)

    @responses.activate
    def test_get_incremental_tickets(self):
        """Test get incremental tickets method."""
        responses.add(
            responses.GET,
            "https://test-company.zendesk.com/api/v2/incremental/tickets/cursor.json",
            json={
                "tickets": [{"id": 1}, {"id": 2}],
                "end_of_stream": False,
                "after_cursor": "test_cursor"
            },
            status=200
        )
        
        result = self.client.get_incremental_tickets(1640995200)
        
        self.assertEqual(len(result["tickets"]), 2)
        self.assertFalse(result["end_of_stream"])
        self.assertEqual(result["after_cursor"], "test_cursor")


class ZendeskConfigurationTestCase(unittest.TestCase):
    """Test different configuration scenarios based on templates."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_tickets_closed_configuration(self):
        """Test configuration for closed tickets (like tickets_closed.json template)."""
        config = ConnectorConfig(
            subdomain="example",
            identity="test@example.com",
            key="token",
            statuses=["closed", "solved"],
            delay_minutes=5,
            name="zendesk-closed-tickets",
            connector="zendesk_tickets"
        )
        
        connector = TicketsConnector(
            config=config,
            context={"runtime": "test_harness", "runtime_id": "NA"}
        )
        
        self.assertEqual(connector.subdomain, "example")
        self.assertEqual(connector.delay_minutes, 5)

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_tickets_open_configuration(self):
        """Test configuration for open tickets (like tickets_open.json template)."""
        config = ConnectorConfig(
            subdomain="example",
            identity="test@example.com",
            key="token",
            statuses=["new", "open", "pending"],
            delay_minutes=5,
            name="zendesk-open-tickets",
            connector="zendesk_tickets"
        )
        
        connector = TicketsConnector(
            config=config,
            context={"runtime": "test_harness", "runtime_id": "NA"}
        )
        
        self.assertEqual(connector.subdomain, "example")
        self.assertEqual(connector.delay_minutes, 5)

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_search_custom_configuration(self):
        """Test configuration for custom search (like search_custom.json template)."""
        config = ConnectorConfig(
            subdomain="example",
            identity="test@example.com",
            key="token",
            status="pending",  # Custom status for search
            include_comments=True,
            delay_minutes=10,
            name="zendesk-search-custom",
            connector="zendesk_search"
        )
        
        connector = SearchConnector(
            config=config,
            context={"runtime": "test_harness", "runtime_id": "NA"}
        )
        
        self.assertEqual(connector.subdomain, "example")
        self.assertEqual(connector.ticket_status, "pending")
        self.assertTrue(connector.include_comments)
        self.assertEqual(connector.delay_minutes, 10)

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_search_closed_configuration(self):
        """Test configuration for closed search (like search_closed.json template)."""
        config = ConnectorConfig(
            subdomain="example",
            identity="test@example.com",
            key="token",
            statuses=["closed", "solved"],
            delay_minutes=5,
            batch_size=25,
            name="zendesk-search-closed",
            connector="zendesk_search"
        )
        
        connector = SearchConnector(
            config=config,
            context={"runtime": "test_harness", "runtime_id": "NA"}
        )
        
        self.assertEqual(connector.subdomain, "example")
        self.assertEqual(connector.delay_minutes, 5)

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_tickets_single_status_configuration(self):
        """Test configuration for single status tickets (like tickets_single_status.json template)."""
        config = ConnectorConfig(
            subdomain="example",
            identity="test@example.com", 
            key="token",
            delay_minutes=5,
            name="zendesk-single-status",
            connector="zendesk_tickets"
        )
        
        connector = TicketsConnector(
            config=config,
            context={"runtime": "test_harness", "runtime_id": "NA"}
        )
        
        self.assertEqual(connector.subdomain, "example")
        self.assertEqual(connector.delay_minutes, 5)
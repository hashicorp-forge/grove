# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Zitadel Events connector."""


import os
import re
import unittest
from unittest.mock import MagicMock, patch

import responses
from responses import matchers

from grove.connectors.zitadel.events import Connector
from grove.exceptions import (
    ConfigurationException,
    RateLimitException,
    RequestFailedException,
)
from grove.models import ConnectorConfig
from tests import mocks


class ZitadelEventsTestCase(unittest.TestCase):
    """Implements unit tests for the Zitadel Events connector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                name="test",
                connector="test",
                identity="https://zitadel.example.com",
                key="test_pat",
                batch_size=100,
                timeout=1,
                aggregate_event_types=["user.created", "user.updated"],
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_collect_success(self):
        """Ensure collect works as expected when API returns valid data."""
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            json={
                "events": [{"sequence": "1", "type": "user.created"}],
                "next_sequence": "2",
            },
            status=200,
            content_type="application/json",
        )

        self.connector.pointer = "0"
        self.connector.save = MagicMock()

        self.connector.collect()

        self.connector.save.assert_called_once_with(
            [{"sequence": "1", "type": "user.created"}]
        )

    @responses.activate
    def test_collect_rate_limit(self):
        """Ensure collect handles rate-limiting correctly."""
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=429,
            headers={"Retry-After": "1"},
        )

        self.connector.pointer = "0"
        with self.assertRaises(RateLimitException):
            self.connector.collect()

    @responses.activate
    def test_collect_server_error(self):
        """Ensure collect raises an exception on server error."""
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=500,
        )

        self.connector.pointer = "0"
        with self.assertRaises(RequestFailedException):
            self.connector.collect()

    def test_build_query(self):
        """Ensure query is built correctly."""
        query = self.connector._build_query(last_sequence="10")
        self.assertEqual(
            query,
            {
                "limit": 100,
                "asc": True,
                "sequence": "10",
                "aggregateTypes": ["user.created", "user.updated"],
            },
        )

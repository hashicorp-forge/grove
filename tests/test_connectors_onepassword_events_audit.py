# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the 1Password Audit Events collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.onepassword.events_audit import Connector
from grove.models import ConnectorConfig
from tests import mocks


class OnePasswordItemUsageEventTestCase(unittest.TestCase):
    """Implements unit tests for the 1Password Audit Events collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="examplecorp",
                key="0123456789",
                name="examplecorp",
                connector="test",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_client_rate_limit(self):
        """Ensure ratelimit waiting is working as expected."""
        # Rate limit the first request.
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=429,
            content_type="application/json",
            body=bytes(),
        )

        # Succeed on the second.
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/onepassword/events_audit/001.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Ensure we sleep appropriately on rate-limit.
        with patch("time.sleep", return_value=None) as mock_sleep:
            self.connector.run()
            mock_sleep.assert_called_with(1)

    @responses.activate
    def test_collect_no_pagination(self):
        """Ensure collection without pagination is working as expected."""
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/onepassword/events_audit/001.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Ensure only a single value is returned, and the pointer is properly set.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 3)
        self.assertEqual(self.connector.pointer, "2023-03-15T16:50:50-03:00")

    @responses.activate
    def test_collect_pagination(self):
        """Ensure collection with pagination is working as expected."""
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/onepassword/events_audit/002.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/onepassword/events_audit/003.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Ensure only a single value is returned, and the pointer is properly set.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 1)
        self.assertEqual(self.connector.pointer, "2023-03-15T16:33:50-03:00")

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the GitHub Audit collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.github.audit_log import Connector
from grove.models import ConnectorConfig
from tests import mocks


class GitHubAuditTestCase(unittest.TestCase):
    """Implements unit tests for the GitHub Audit collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="examplecorp",
                key="ghp_c0ffeec0ffeec0ffeec0ffeec0ffee",
                name="examplecorp",
                connector="test",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_client_rate_limit_403(self):
        """Ensure ratelimit waiting is working as expected."""
        # Rate limit the first request.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=403,
            content_type="application/json",
            body=bytes(),
            headers={
                "X-RateLimit-Remaining": "0",
                "x-ratelimit-reset": "0",  # Unix-time, so definitely in the past :)
            },
        )

        # Succeed on the second.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/github/audit/001.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        # Ensure we sleep appropriately on rate-limit. This takes advantage of the retry
        # logic sleeping for one second if the ratelimit-reset header is in the past.
        with patch("time.sleep", return_value=None) as mock_sleep:
            self.connector.run()
            mock_sleep.assert_called_with(1)

    @responses.activate
    def test_client_rate_limit_429(self):
        """Ensure ratelimit waiting is working as expected."""
        # Rate limit the first request.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=429,
            content_type="application/json",
            body=bytes(),
            headers={
                "X-RateLimit-Remaining": "0",
                "x-ratelimit-reset": "0",  # Unix-time, so definitely in the past :)
            },
        )

        # Succeed on the second.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/github/audit/001.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        # Ensure we sleep appropriately on rate-limit. This takes advantage of the retry
        # logic sleeping for one second if the ratelimit-reset header is in the past.
        with patch("time.sleep", return_value=None) as mock_sleep:
            self.connector.run()
            mock_sleep.assert_called_with(1)

    @responses.activate
    def test_collect_no_pagination(self):
        """Ensure collection without pagination is working as expected."""
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/github/audit/001.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        # Ensure the correct number of value are returned, and the pointer properly set.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 2)
        self.assertEqual(self.connector.pointer, "1625045793361")

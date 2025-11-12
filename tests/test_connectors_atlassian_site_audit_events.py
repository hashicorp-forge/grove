# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Atlassian Audit collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.atlassian_site.audit_events import Connector
from grove.exceptions import RequestFailedException
from grove.models import ConnectorConfig
from tests import mocks


class AtlassianSiteEventAuditTestCase(unittest.TestCase):
    """Implements integration tests for the Atlassian Audit collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="corporation",
                key="token",
                username="username@username.com",
                name="test",
                connector="atlassian_site_test",
            ),
            context={
                "runtime": "test_site_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_client_rate_limit(self):
        """Ensure ratelimit waiting is working as expected."""
        # Rate limit the first request.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=429,
            content_type="application/json",
            body=bytes(),
            headers={
                "X-Rate-Limit-Remaining": "0",
                "X-Rate-Limit-Reset": "0",  # Unix-time, so definitely in the past :)
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
                    os.path.join(
                        self.dir, "fixtures/atlassian_site/event_audit/002.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Ensure time.sleep is called with the correct value in response to a
        # rate-limit.
        with patch("time.sleep", return_value=None) as mock_sleep:
            self.connector.collect()
            mock_sleep.assert_called_with(1)

    @responses.activate
    def test_get_token_fail_on_server_error(self):
        """Ensure server errors raise an appropriate exception."""
        # Setup an fake HTTP 400 response from Atlassian site.
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=400,
            content_type="application/json",
            body=bytes(),
        )

        with self.assertRaises(RequestFailedException):
            self.connector.collect()

    @responses.activate
    def test_collect_fail_on_server_error(self):
        """Ensure server errors raise an appropriate exception."""
        # Get bearer token
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/atlassian_site/event_audit/001.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        # Setup an fake HTTP 500 response from Atlassian site.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=500,
            body=bytes(),
        )

        with self.assertRaises(RequestFailedException):
            self.connector.collect()

    @responses.activate
    def test_collect_pagination(self):
        """Ensure pagination is working as expected."""
        # Succeed with a cursor returned (to indicate paging is required).
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/atlassian_site/event_audit/002.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # The last "page" returns an empty cursor.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/atlassian_site/event_audit/004.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Check the pointer matches the latest execution_time value, and that the
        # expected number of logs were returned.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 2)
        self.assertEqual(self.connector.pointer, "2025-11-12T16:28:36.918+0000")

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
                    os.path.join(
                        self.dir, "fixtures/atlassian_site/event_audit/004.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Ensure only a single value is returned, and the pointer is properly set.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 3)
        self.assertEqual(self.connector.pointer, "2025-11-12T16:28:10.570+0000")

    @responses.activate
    def test_collect_no_results(self):
        """Ensure break accurs when there is no results returned."""
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/atlassian_site/event_audit/003.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 0)

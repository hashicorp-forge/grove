# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements integration tests for the Workday Audit collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.workday.activity_logging import Connector
from grove.exceptions import RequestFailedException
from grove.models import ConnectorConfig
from tests import mocks


class WorkdayActivityLogging(unittest.TestCase):
    """Implements integration tests for the Workday Audit collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                base_url="test.workday.com",
                client_id="000000",
                client_secret="EEEEE",
                identity="corporation",
                key="token",
                name="test",
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
        # Get bearer token
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/workday/activity_logging/001.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
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
                        self.dir, "fixtures/workday/activity_logging/004.json"
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
        # Setup an fake HTTP 400 response from WorkDay.
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
                        self.dir, "fixtures/workday/activity_logging/001.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        # Setup an fake HTTP 500 response from WorkDay.
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
        # Get bearer token
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/workday/activity_logging/001.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        # Succeed with a cursor returned (to indicate paging is required).
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/workday/activity_logging/002.json"
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
                        self.dir, "fixtures/workday/activity_logging/004.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Check the pointer matches the latest execution_time value, and that the
        # expected number of logs were returned.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 113)
        self.assertEqual(self.connector.pointer, "2021-10-12T23:50:09.752Z")

    @responses.activate
    def test_collect_no_pagination(self):
        """Ensure collection without pagination is working as expected."""
        # Get bearer token
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/workday/activity_logging/001.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/workday/activity_logging/004.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Ensure only a single value is returned, and the pointer is properly set.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 13)
        self.assertEqual(self.connector.pointer, "2021-10-12T23:50:09.752Z")

    @responses.activate
    def test_collect_no_results(self):
        """Ensure break accurs when there is no results returned."""
        # Get bearer token
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/workday/activity_logging/001.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(
                        self.dir, "fixtures/workday/activity_logging/003.json"
                    ),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 0)

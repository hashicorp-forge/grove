# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the PagerDuty audit records collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.pagerduty.audit_records import Connector
from grove.models import ConnectorConfig
from tests import mocks


class PagerDutyAuditTestCase(unittest.TestCase):
    """Implements tests for the PagerDuty audit records collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="1FEEDFEED1",
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
    def test_collect_rate_limit(self):
        """Ensure rate-limit retires are working as expected."""
        # Rate limit the first request.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=429,
            content_type="application/json",
            body=bytes(),
        )

        # Succeed on the second.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/pagerduty/audit_records/003.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Ensure time.sleep is called with the correct value in response to a
        # rate-limit.
        with patch("time.sleep", return_value=None) as mock_sleep:
            self.connector.run()
            mock_sleep.assert_called_with(1)

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
                    os.path.join(self.dir, "fixtures/pagerduty/audit_records/001.json"),
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
                    os.path.join(self.dir, "fixtures/pagerduty/audit_records/002.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Check the pointer matches the latest execution_time value, and that the
        # expected number of logs were returned.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 5)
        self.assertEqual(self.connector.pointer, "2021-09-08T18:03:32.120Z")

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
                    os.path.join(self.dir, "fixtures/pagerduty/audit_records/003.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Set the chunk size large enough that no chunking is required.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 4)
        self.assertEqual(self.connector.pointer, "2021-09-08T18:05:45.120Z")

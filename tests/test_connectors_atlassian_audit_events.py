# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Atlassian Audit collector."""

import os
import re
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import responses

from grove.connectors.atlassian.api import API_DATE_FORMAT
from grove.connectors.atlassian.audit_events import Connector
from grove.models import ConnectorConfig
from tests import mocks


class AtlassianEventAuditTestCase(unittest.TestCase):
    """Implements integration tests for the Atlassian Audit collector."""

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
    def test_collect_pagination(self):
        """Ensure pagination is working as expected."""
        # Succeed with a cursor returned (to indicate paging is required).
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(os.path.join(self.dir, "fixtures/atlassian/event_audit/001.json"), "r").read(), # noqa: E501
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
                open(os.path.join(self.dir, "fixtures/atlassian/event_audit/002.json"), "r").read(), # noqa: E501
                "utf-8",
            ),
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 31)
        self.assertEqual(self.connector.pointer, "2022-05-12T19:13:13Z")

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
                    os.path.join(self.dir, "fixtures/atlassian/event_audit/002.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 1)
        self.assertEqual(self.connector.pointer, "2022-05-12T19:13:13Z")

    @responses.activate
    def test_client_rate_limit_429(self):
        """Ensure ratelimit waiting is working as expected."""
        now = datetime.utcnow()
        later = now + timedelta(minutes=1)

        # Rate limit the first request.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=429,
            content_type="application/json",
            body=bytes(),
            headers={
                "X-Ratelimit-Reset": f"{later.strftime(API_DATE_FORMAT)}Z",
            },
        )

        # Succeed on the second.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(os.path.join(self.dir, "fixtures/atlassian/event_audit/002.json"), "r").read(), # noqa: E501
                "utf-8",
            ),
        )

        # Ensure we sleep appropriately on rate-limit. This takes advantage of the retry
        # logic sleeping for one second if the ratelimit-reset header is in the past.
        with patch("time.sleep", return_value=None) as mock_sleep:
            self.connector.run()
            mock_sleep.assert_called_once()

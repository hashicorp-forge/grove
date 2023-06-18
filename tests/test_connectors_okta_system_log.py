# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Okta SystemLog API collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.okta.system_log import Connector
from grove.models import ConnectorConfig
from tests import mocks


class OktaAuditTestCase(unittest.TestCase):
    """Implements unit tests for the Okta SystemLog Audit collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="example",
                key="token",
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
                    os.path.join(self.dir, "fixtures/okta/system_log/001.json"), "r"
                ).read(),
                "utf-8",
            ),
        )
        # Ensure only a single value is returned, and the pointer is properly set.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 1)
        self.assertEqual(self.connector.pointer, "2021-06-24T00:04:08.123Z")

    @responses.activate
    def test_collect_no_results(self):
        """Ensure break occurs when there is no results returned."""
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/okta/system_log/002.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

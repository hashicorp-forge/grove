# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements integration tests for the GSuite Alerts collector."""

import os
import unittest
from unittest.mock import patch

from googleapiclient.http import HttpMockSequence

from grove.connectors.gsuite.alerts import Connector
from grove.models import ConnectorConfig
from tests import mocks


class MockSequence(HttpMockSequence):
    def close(self, *args, **kwargs):
        pass


class GSuiteAlertsTestCase(unittest.TestCase):
    """Implements integration tests for the GSuite Alerts collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="1FEEDFEED1",
                key="{}",
                name="test",
                connector="test",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @patch("google.oauth2.service_account.Credentials.from_service_account_info")
    @patch("googleapiclient.discovery.build")
    @patch("grove.connectors.gsuite.alerts.Connector.get_http_transport")
    def test_collect_pagination(self, mock_transport, mock_request, mock_auth):
        """Ensure collection works as expected."""
        mock_transport.return_value = MockSequence(
            [
                (
                    {"status": "200"},
                    open(
                        os.path.join(self.dir, "fixtures/gsuite/alerts/001.json"),
                        "r",
                    ).read(),
                ),
                (
                    {"status": "200"},
                    open(
                        os.path.join(self.dir, "fixtures/gsuite/alerts/002.json"),
                        "r",
                    ).read(),
                ),
            ],
        )
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 2)
        self.assertEqual(self.connector.pointer, "2021-04-03T14:05:39.950458Z")

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements integration tests for the GSuite Activities collector."""

import os
import unittest
from unittest.mock import patch

from googleapiclient.http import HttpMockSequence

from grove.connectors.gsuite.activities import Connector
from grove.models import ConnectorConfig
from tests import mocks


class MockSequence(HttpMockSequence):
    def close(self, *args, **kwargs):
        pass


class GSuiteActivitiesTestCase(unittest.TestCase):
    """Implements integration tests for the GSuite Activities collector."""

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
                operation="admin",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @patch("google.oauth2.service_account.Credentials.from_service_account_info")
    @patch("googleapiclient.discovery.build")
    @patch("grove.connectors.gsuite.activities.Connector.get_http_transport")
    def test_collect_pagination(self, mock_transport, mock_request, mock_auth):
        """Ensure collection works as expected."""
        mock_transport.return_value = MockSequence(
            [
                (
                    {"status": "200"},
                    open(
                        os.path.join(self.dir, "fixtures/gsuite/activities/001.json"),
                        "r",
                    ).read(),
                ),
                (
                    {"status": "200"},
                    open(
                        os.path.join(self.dir, "fixtures/gsuite/activities/002.json"),
                        "r",
                    ).read(),
                ),
            ],
        )
        self.connector.run()

        self.assertEqual(self.connector._saved["logs"], 2)
        self.assertEqual(self.connector.pointer, "2021-10-27T23:59:31.657Z")

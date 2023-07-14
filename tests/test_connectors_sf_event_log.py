# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements integration tests for the SalesForce Event Log collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.sf.event_log import Connector
from grove.exceptions import RequestFailedException
from grove.models import ConnectorConfig
from tests import mocks


class SFEventLogTestCase(unittest.TestCase):
    """Implements integration tests for the SalesForce Evennt Log collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="Someuser",
                key="token",
                name="test",
                connector="test",
                token="12345",
                operation="Login",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_collect_fail_on_server_error(self):
        """Ensure server errors raise an appropriate exception."""
        # Ensure authentication succeeds (POST to SF).
        login_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/login.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            body=login_response,
            content_type="application/xml",
        )

        # Ensure first query returns an HTTP 500 (GET to SF)
        query_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/error.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=500,
            body=query_response,
            content_type="application/xml",
        )

        with self.assertRaises(RequestFailedException):
            self.connector.collect()

    @responses.activate
    def test_collect_no_pagination(self):
        """Ensure collection without pagination is working as expected."""
        # Ensure authentication succeeds (POST to SF).
        login_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/login.xml"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            body=login_response,
            content_type="application/xml",
        )

        # Ensure EventLogFile query returns a 200 (GET to SF).
        query_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/001.json"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            body=query_response,
            content_type="application/xml",
        )

        # Ensure LogFile query returns a 200 (GET to SF).
        log_response = bytes(
            open(os.path.join(self.dir, "fixtures/sf/event_log/001.csv"), "r").read(),
            "utf-8",
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            body=log_response,
            content_type="text/csv",
        )

        # Check the pointer matches the latest value, and that the expected number of
        # logs were returned.
        self.connector.collect()
        self.assertEqual(self.connector._saved["logs"], 2)
        self.assertEqual(self.connector.pointer, "2038-01-19T03:00:00.000Z")

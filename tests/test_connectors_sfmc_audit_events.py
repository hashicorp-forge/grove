"""Implements tests for the SalesForce Marketing Cloud audit event collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.sfmc.audit_events import Connector
from grove.models import ConnectorConfig
from tests import mocks


class SFMCAuditTestCase(unittest.TestCase):
    """Implements tests for the SalesForce Marketing Cloud audit event collector."""

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
                open(
                    os.path.join(self.dir, "fixtures/sfmc/audit_events/001.json"), "r"
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
                    os.path.join(self.dir, "fixtures/sfmc/audit_events/002.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        # Check the pointer matches the latest value, and that the expected number of
        # logs were returned.
        self.connector.collect()
        self.assertEqual(self.connector._saved, 2)
        self.assertEqual(self.connector.pointer, "2019-01-02T12:00:00.00")

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
                    os.path.join(self.dir, "fixtures/sfmc/audit_events/003.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        self.connector.collect()
        self.assertEqual(self.connector._saved, 7)
        self.assertEqual(self.connector.pointer, "2019-01-07T12:00:00.00")

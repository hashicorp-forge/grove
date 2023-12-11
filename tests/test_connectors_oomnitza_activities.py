# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Oomnitza Audit collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.oomnitza.activities import Connector
from grove.models import ConnectorConfig
from tests import mocks


class OomnitzaAuditTestCase(unittest.TestCase):
    """Implements unit tests for the Oomnitza Activities collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="corp",
                key="testkey",
                name="corp",
                connector="test",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    @patch("grove.connectors.oomnitza.api.API_PAGE_SIZE", 200)
    def test_collect_pagination(self):
        """Ensure pagination is working as expected."""
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/oomnitza/activities/002.json"),
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
                    os.path.join(self.dir, "fixtures/oomnitza/activities/001.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Check the pointer matches the latest execution_time value, and that the
        # expected number of logs were returned.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 205)
        self.assertEqual(self.connector.pointer, "1682538024")

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
                    os.path.join(self.dir, "fixtures/oomnitza/activities/001.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Ensure only a single value is returned, and the pointer is properly set.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 5)
        self.assertEqual(self.connector.pointer, "1680895957")

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
                    os.path.join(self.dir, "fixtures/oomnitza/activities/003.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 0)

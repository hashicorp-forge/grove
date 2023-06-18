# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Zoom Activity Log collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses
from responses import matchers

from grove.connectors.zoom.activities import Connector
from grove.models import ConnectorConfig
from tests import mocks


class ZoomActivitiesTestCase(unittest.TestCase):
    """Implements unit tests for the Zoom Activity Logs collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="client_id",
                key="test_key",
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
        # set bearer token
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/zoom/client/001.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        # first page has a cursor to the next page
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/zoom/activities/001.json"), "r"
                ).read(),
                "utf-8",
            ),
            match=[matchers.header_matcher({"Authorization": "Bearer testtoken"})],
        )

        # The last page returns no cursor.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/zoom/activities/002.json"), "r"
                ).read(),
                "utf-8",
            ),
            match=[matchers.header_matcher({"Authorization": "Bearer testtoken"})],
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 31)
        self.assertEqual(self.connector.pointer, "2022-08-23T14:45:54Z")

    @responses.activate
    def test_collect_no_pagination(self):
        """Ensure collection without pagination is working as expected."""
        # set bearer token
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/zoom/client/001.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # add one entry result with no cursor
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/zoom/activities/002.json"), "r"
                ).read(),
                "utf-8",
            ),
            match=[matchers.header_matcher({"Authorization": "Bearer testtoken"})],
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 1)
        self.assertEqual(self.connector.pointer, "2022-08-23T14:45:54Z")

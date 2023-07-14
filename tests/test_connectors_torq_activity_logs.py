# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Torq Activity Log collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses
from responses import matchers

from grove.connectors.torq.activity_logs import Connector
from grove.models import ConnectorConfig
from tests import mocks


class TorqActivityLogsTestCase(unittest.TestCase):
    """Implements unit tests for the Torq Activity Logs collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="client_id",
                key="client_secret",
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
                    os.path.join(self.dir, "fixtures/torq/client/001.json"),
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
                    os.path.join(self.dir, "fixtures/torq/activity/001.json"), "r"
                ).read(),
                "utf-8",
            ),
            match=[
                matchers.header_matcher(
                    {"Authorization": f"Bearer aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
                )
            ],
        )

        # The last page returns no cursor.
        responses.add(
            responses.GET,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/torq/activity/002.json"), "r"
                ).read(),
                "utf-8",
            ),
            match=[
                matchers.header_matcher(
                    {"Authorization": f"Bearer aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
                )
            ],
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 2)
        # it's reverse chronological so the earlier timestamp should be recorded
        self.assertEqual(self.connector.pointer, "2022-06-24T18:15:07.380622Z")

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
                    os.path.join(self.dir, "fixtures/torq/client/001.json"),
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
                    os.path.join(self.dir, "fixtures/torq/activity/002.json"), "r"
                ).read(),
                "utf-8",
            ),
            match=[
                matchers.header_matcher(
                    {"Authorization": f"Bearer aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
                )
            ],
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 1)
        self.assertEqual(self.connector.pointer, "2022-06-24T18:15:06.380622Z")

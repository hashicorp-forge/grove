# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements integration tests for the Stripe Events collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.stripe.events import Connector
from grove.models import ConnectorConfig
from tests import mocks


class StripeEventsTestCase(unittest.TestCase):
    """Implements integration tests for the Stripe Events collector."""

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
                    os.path.join(self.dir, "fixtures/stripe/events/001.json"), "r"
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
                    os.path.join(self.dir, "fixtures/stripe/events/002.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 2)
        self.assertEqual(self.connector.pointer, "1728005905")

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
                    os.path.join(self.dir, "fixtures/stripe/events/002.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 1)
        self.assertEqual(self.connector.pointer, "1727897773")

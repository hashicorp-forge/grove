# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for connector event deduplication."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove import constants
from grove.connectors import ConnectorConfig
from tests import mocks
from tests.connectors.test import Connector


class ConnectorDeduplicationTestCase(unittest.TestCase):
    """Implements unit tests for connector event deduplication."""

    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))

    def add_response(self, path: str = None, status: int = 200):
        """Adds a response to responses."""
        body = None
        if path:
            body = bytes(open(path, "r").read(), "utf-8")

        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=status,
            content_type="application/json",
            body=body,
        )

    @responses.activate
    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_deduplication_chronological(self):
        """Ensure deduplication works as expected for chronological data."""
        config = ConnectorConfig(
            identity="ORGANISATION",
            key="SOMETHING_SECURE",
            name="UNIT_TEST",
            connector="test_only_connector",
        )
        context = {
            "runtime": "test_harness",
            "runtime_id": "NA",
        }
        first_collection = Connector(config=config, context=context)

        # Load all simulated responses in order.
        for file in ["001.json", "002.json", "003.json"]:
            self.add_response(
                path=os.path.join(self.dir, f"fixtures/grove/chronological/{file}")
            )

        # Run a full collection first and ensure all is as we expect.
        first_collection.run()
        self.assertEqual(first_collection._saved["logs"], 7)
        self.assertEqual(first_collection.pointer, "7")

        # Perform a collection with the latest pointer, and ensure no new records are
        # saved - as they're duplicates. This simulates a "return logs inclusive of the
        # current pointer" operation.
        second_collection = Connector(config=config, context=context)

        # In-memory cache does not support locking as it's only intended for local
        # "one-shot" execution, and development use. As a result, we have to currently
        # alias one to the other to simulate this.
        #
        # TODO: Remove the need for this, as it's going to cause confusion in future.
        second_collection._cache._data = first_collection._cache._data

        self.add_response(
            path=os.path.join(self.dir, "fixtures/grove/chronological/003.json")
        )

        second_collection.run()
        self.assertEqual(second_collection._saved["logs"], 0)
        self.assertEqual(second_collection.pointer, "7")

    @responses.activate
    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_deduplication_reverse_chronological(self):
        """Ensure deduplication works as expected for reverse chronological data."""
        config = ConnectorConfig(
            identity="ORGANISATION",
            key="SOMETHING_SECURE",
            name="UNIT_TEST",
            connector="test_only_connector",
        )
        context = {
            "runtime": "test_harness",
            "runtime_id": "NA",
        }

        # Hot patch the connector to work in reverse chronological order.
        first_collection = Connector(config=config, context=context)

        # This is very naughty.
        first_collection.LOG_ORDER = constants.REVERSE_CHRONOLOGICAL

        # Load all simulated responses in order.
        for file in ["001.json", "002.json"]:
            self.add_response(
                path=os.path.join(
                    self.dir, f"fixtures/grove/reverse_chronological/{file}"
                )
            )

        # Run a full collection first and ensure all is as we expect.
        first_collection.run()
        self.assertEqual(first_collection._saved["logs"], 7)
        self.assertEqual(first_collection.pointer, "7")

        # Perform a collection with the latest pointer, and ensure that only new records
        # are returned.
        second_collection = Connector(config=config, context=context)

        # In-memory cache does not support locking as it's only intended for local
        # "one-shot" execution, and development use. As a result, we have to currently
        # alias the state of one cache to the other to simulate this.
        #
        # TODO: Remove the need for this, as it's going to cause confusion in future.
        second_collection._cache._data = first_collection._cache._data

        self.add_response(
            path=os.path.join(self.dir, "fixtures/grove/reverse_chronological/003.json")
        )

        second_collection.run()
        self.assertEqual(second_collection._saved["logs"], 1)
        self.assertEqual(second_collection.pointer, "7")

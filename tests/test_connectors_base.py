# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the Base connector."""

import unittest
from unittest.mock import patch

from grove.connectors import BaseConnector
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException
from grove.models import ConnectorConfig
from tests import mocks


class BaseConnectorTestCase(unittest.TestCase):
    """Implements tests for the Base connector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_pointers(self):
        """Ensures pointers can be retrieved without issue."""
        first = BaseConnector(
            config=ConnectorConfig(
                key="token",
                name="test",
                identity="1FEEDFEED1",
                connector="example_one",
                operation="first",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        second = BaseConnector(
            config=ConnectorConfig(
                key="token",
                name="test",
                identity="1FEEDFEED1",
                connector="example_one",
                operation="second",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

        # Ensure the first pointer being set and saved doesn't interfere with the
        # second - as the operations differ.
        first.pointer = "AAAAAAAA"
        self.assertEqual(first.pointer, "AAAAAAAA")

        # Ensure no pointer is found for the second operation.
        with self.assertRaises(NotFoundException):
            _ = second.pointer

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_plain_key(self):
        """Ensures keys are used directly if not set as a secret."""
        first = BaseConnector(
            config=ConnectorConfig(
                key="token",
                name="test",
                identity="1FEEDFEED1",
                connector="example_one",
                operation="second",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        self.assertEqual(first.key, "token")

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_encoded_key(self):
        """Ensures key encoding is handled."""
        # Ensure keys are base64 decoded properly.
        connector = BaseConnector(
            config=ConnectorConfig(
                key="QUJDREVG",
                name="test",
                identity="1FEEDFEED1",
                connector="example_one",
                operation="second",
                encoding={
                    "key": "base64",
                },
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        self.assertEqual(connector.key, "ABCDEF")

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_recover_from_incomplete(self):
        # Setup a connector which operates in reverse chronological mode.
        connector = BaseConnector(
            config=ConnectorConfig(
                key="token",
                name="test",
                identity="1FEEDFEED1",
                connector="example_one",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        connector.NAME = "test"
        connector.POINTER_PATH = "time"
        connector.LOG_ORDER = REVERSE_CHRONOLOGICAL

        # Simulates logs returned from an upstream / external service.
        logs = [
            {"time": "2022-11-20T20:38:18.382Z", "name": "AAH"},  #   ^
            {"time": "2022-11-20T20:37:18.382Z", "name": "AAG"},  #   | New logs since
            {"time": "2022-11-20T20:36:18.382Z", "name": "AAF"},  #   | the failed
            {"time": "2022-11-20T20:35:18.382Z", "name": "AAE"},  #   | collection.
            {"time": "2022-11-20T20:34:18.382Z", "name": "AAD"},  #   |
            {"time": "2022-11-20T20:33:18.382Z", "name": "AAC"},  # - /
            {"time": "2022-11-20T20:31:18.382Z", "name": "AAB"},  # <-. Most recent log
            {"time": "2022-11-20T20:30:10.772Z", "name": "AAA"},  #   | from failed run.
        ]

        # Setup the cache to match the described failure case.
        connector._cache.set(
            pk="pointer_next.test.06dc0fd3c08a2bc6a33f5460da9fea10",
            sk="all",
            value="2022-11-20T20:31:18.382Z",
        )
        connector._cache.set(
            pk="window_start.test.06dc0fd3c08a2bc6a33f5460da9fea10",
            sk="all",
            value="2022-11-20T20:31:18.382Z",
        )
        connector._cache.set(
            pk="pointer_previous.test.06dc0fd3c08a2bc6a33f5460da9fea10",
            sk="all",
            value="2022-11-20T20:30:10.772Z",
        )
        connector._cache.set(
            pk="window_end.test.06dc0fd3c08a2bc6a33f5460da9fea10",
            sk="all",
            value="2022-11-20T20:30:10.772Z",
        )
        connector._cache.set(
            pk="pointer.test.06dc0fd3c08a2bc6a33f5460da9fea10",
            sk="all",
            value="2022-11-20T20:31:18.382Z",
        )

        # Simulates operations performed by "run()" without calling. Run is not called
        # as this catches all GroveExceptions to prevent unhandled errors in regular
        # operation, which complicates testing.
        connector.save(logs)
        connector.pointer = connector.pointer_next
        connector.save_hashes()

        # Clean-up to simulate a fresh run without having to re-create everything in
        # a subsequent test case.
        connector._cache.delete(
            pk="window_start.test.06dc0fd3c08a2bc6a33f5460da9fea10",
            sk="all",
        )
        connector._cache.delete(
            pk="window_end.test.06dc0fd3c08a2bc6a33f5460da9fea10",
            sk="all",
        )
        connector._cache.delete(
            pk="pointer_next.test.06dc0fd3c08a2bc6a33f5460da9fea10",
            sk="all",
        )
        connector._window_end = ""
        connector._window_start = ""
        connector._pointer_next = ""
        connector._pointer_previous = ""

        # Remove deduplication hashes from memory, to force getting them from cache
        # simulating a real run.
        connector._hashes = {}

        # Simulates a subsequent collection / execution. This is to ensure that logs are
        # collected as expected after the "recovery" operation has complete.
        connector.save(logs)
        connector.pointer = connector.pointer_next
        connector.save_hashes()

        # The pointer should now be the latest log entry - as we should be caught up.
        self.assertEqual(connector.pointer, logs[0].get("time"))

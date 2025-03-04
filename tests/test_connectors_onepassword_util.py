# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the 1Password get_pointer_values function. This is a common
function that is used to get the proper cursor/timestamp to use."""
import os
import unittest
from datetime import datetime, timedelta

from grove.connectors.onepassword.events_itemusages import Connector
from grove.connectors.onepassword.util import get_pointer_values
from grove.models import ConnectorConfig


class OnePasswordGetPointerValuesTestCase(unittest.TestCase):
    """Implements unit tests for the 1Password get_pointer_values function."""

    def setUp(self):
        """Ensure the application is setup for testing."""
        self.connector = Connector(
            config=ConnectorConfig(
                identity="examplecorp",
                key="0123456789",
                name="examplecorp",
                connector="test",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    def test_check_timestamp_returns_timestamp(self):
        """Tests what happens if get_pointer values is passed a timestamp for a pointer.
        We expect to detect the timestamp and return that with an empty cursor.
        """
        current_time = datetime.now().isoformat()
        self.connector.pointer = current_time
        cursor, start_time = get_pointer_values(self.connector)

        self.assertEqual(cursor, None)
        self.assertEqual(start_time, current_time)

    def test_check_cursor_returns_cursor(self):
        """Tests what happens if get_pointer values is passed a cursor for a pointer.
        We expect to detect the cursor and return that with an empty timestamp.
        """
        test_cursor = "aGVsbG8hIGlzIGl0IG1lIHlvdSBhcmUgbG9va2luZyBmb3IK"
        self.connector.pointer = test_cursor
        cursor, start_time = get_pointer_values(self.connector)

        self.assertEqual(start_time, None)
        self.assertEqual(cursor, test_cursor)

    def test_check_empty_returns_timestamp(self):
        """Tests what happens if get_pointer values isn't passed a pointer.
        We expect it to detect the missing pointer and use a default time to initialize.
        """
        week_ago = datetime.now() - timedelta(days=7)
        test_start_time = (week_ago).astimezone().replace(microsecond=0).isoformat()
        cursor, start_time = get_pointer_values(self.connector)

        self.assertEqual(cursor, None)
        self.assertGreaterEqual(start_time, test_start_time)

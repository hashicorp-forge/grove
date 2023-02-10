# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the local memory cache handler."""

import unittest

from grove.caches.local_memory import Handler
from grove.exceptions import DataFormatException


class LocalMemoryCacheTestCase(unittest.TestCase):
    """Implements tests for the local memory cache handler."""

    def test_set(self):
        """Ensures our set operations match expectations."""
        handler = Handler()

        # Set a value with no constraints.
        handler.set("test", "fixture", "0000")

        # Ensure not_set is working as expected.
        handler.set("test", "fixture", "0001", not_set=False)
        with self.assertRaises(DataFormatException):
            handler.set("test", "fixture", "0002", not_set=True)

        # Ensure constraints are working as expected.
        handler.set("test", "fixture", "0003", constraint="0001")
        with self.assertRaises(DataFormatException):
            handler.set("test", "fixture", "0004", constraint="____")

        # Ensure constraints and not_set are working together.
        handler.set("test", "fixture", "0005", constraint="0003", not_set=False)

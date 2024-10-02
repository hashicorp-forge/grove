# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the local file backed cache handler."""

import os
import tempfile
import unittest

from grove.caches.local_file import Handler
from grove.exceptions import DataFormatException, NotFoundException


class LocalFileCacheTestCase(unittest.TestCase):
    """Implements tests for the local file backed cache handler."""

    def setUp(self):
        """Create temporary directories before each test."""
        self.path = tempfile.TemporaryDirectory()
        os.environ["GROVE_CACHE_LOCAL_FILE_PATH"] = self.path.name

    def tearDown(self):
        """Clean-up temporary directories after each test."""
        self.path.cleanup()

    def test_delete(self):
        """Ensures our delete operations match expectations."""
        handler = Handler()

        # Ensures we can delete existing cache values.
        expected = "0000"

        handler.set("test", "fixture", expected)
        handler.delete("test", "fixture")

        # Ensures we can delete existing cache values - with constraint.
        expected = "0001"
        handler.set("test", "fixture", expected)
        handler.delete("test", "fixture", constraint=expected)

        # Ensures deletion failes if the constraint doesn't match.
        expected = "0002"

        handler.set("test", "fixture", expected)
        with self.assertRaises(DataFormatException):
            handler.delete("test", "fixture", constraint="0000")

    def test_get(self):
        """Ensures our get operations match expectations."""
        handler = Handler()

        # Ensure a non-existent value raises.
        with self.assertRaises(NotFoundException):
            handler.get("test", "fixture")

        # Ensures values can be set and get, returning the correct value.
        expected = "0000"

        handler.set("test", "fixture", expected)
        candidate = handler.get("test", "fixture")

        self.assertEqual(candidate, expected)

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

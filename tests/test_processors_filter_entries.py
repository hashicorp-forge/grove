# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the entry filtering processor."""

import json
import os
import unittest
from unittest.mock import patch

from grove.models import ProcessorConfig
from grove.processors import filter_entries
from tests import mocks


class ProcessorPathFilterTestCase(unittest.TestCase):
    """Implements unit tests for the entry filtering processor."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Setup the processor and associated configuration for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))

        # Create a mapping compatible with the Github Git log fixtures.
        self.processor = filter_entries.Handler(
            ProcessorConfig(
                name="Filter entries with no actor",
                processor="filter_entries",
                filters=[
                    "actor == null",
                ],
            )
        )

    def test_filter_entries(self):
        """Ensure entry filtering operates as expected."""
        # Load and process the fixture.
        entries = json.load(
            open(os.path.join(self.dir, "fixtures/github/git/001.json"), "r")
        )

        # Firstly, ensure four records exist to begin with.
        self.assertEqual(len(entries), 4)

        records = []
        for entry in entries:
            records.extend(self.processor.process(entry))

        # Ensure records with no actor were dropped.
        self.assertEqual(len(records), 1)
        self.assertIn("actor", records[0])

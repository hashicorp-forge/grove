# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the path splitting processor."""

import json
import os
import unittest
from unittest.mock import patch

from grove.models import ProcessorConfig
from grove.processors import split_path
from tests import mocks


class ProcessorPathMapperTestCase(unittest.TestCase):
    """Implements unit tests for the path splitting processor."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Setup the processor and associated configuration for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))

        # Create a mapping compatible with the Okta system log fixture.
        self.processor = split_path.Handler(
            ProcessorConfig(
                name="Fan Out",
                processor="split_path",
                source="events",
            )
        )

    def test_split_path(self):
        """Ensure path splitting operates as expected."""
        # Load and process the fixture.
        entries = json.load(
            open(
                os.path.join(self.dir, "fixtures/gsuite/activities/003.json"),
                "r",
            )
        )

        # Strip the outer items list from the input logs, as Grove will have stripped
        # this already.
        entries = entries.get("items", [])

        # Confirm that the initial log entry has one entry.
        self.assertEqual(len(entries), 1)

        # Process the entries.
        records = []
        for entry in entries:
            records.extend(self.processor.process(entry))

        # Confirm that five records resulted after splitting.
        self.assertEqual(len(records), 5)

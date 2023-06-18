# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the path filtering processor."""

import json
import os
import unittest
from unittest.mock import patch

from grove.models import ProcessorConfig
from grove.processors import filter_paths
from tests import mocks


class ProcessorPathFilterTestCase(unittest.TestCase):
    """Implements unit tests for the path filtering processor."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Setup the processor and associated configuration for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))

        # Create a mapping compatible with the Okta system log fixture.
        self.processor = filter_paths.Handler(
            ProcessorConfig(
                name="Filter debugContext",
                processor="filter_paths",
                sources=[
                    "debugContext",
                    "client.geographicalContext",
                ],
            )
        )

    def test_filter_paths(self):
        """Ensure path filtering operates as expected."""
        # Load and process the fixture.
        entries = json.load(
            open(
                os.path.join(self.dir, "fixtures/okta/system_log/001.json"),
                "r",
            )
        )

        # Firstly, ensure the 'debugContext' field exists to begin with.
        self.assertTrue("debugContext" in entries[0])
        self.assertTrue("geographicalContext" in entries[0]["client"])

        # Process a single log entry.
        records = self.processor.process(entries[0])

        # Ensure the 'debugContext' field was removed.
        self.assertFalse("debugContext" in records[0])
        self.assertFalse("geographicalContext" in entries[0]["client"])

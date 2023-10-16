# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the path zipping processor."""

import json
import os
import unittest
from unittest.mock import patch

from grove.models import ProcessorConfig
from grove.processors import zip_paths
from tests import mocks


class ProcessorZipPathsTestCase(unittest.TestCase):
    """Implements unit tests for the path zipping processor."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Setup the processor and associated configuration for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))

        # Create a mapping compatible with the Okta system log fixture.
        self.processor = zip_paths.Handler(
            ProcessorConfig(
                name="Zip parameters",
                processor="zip_paths",
                source="events.parameters",
                key="name",
                values=[
                    "value",
                    "boolValue",
                    "multiValue",
                ],
            )
        )

    def test_zip_paths(self):
        """Ensure path zipping operates as expected."""
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

        # Modify the entry to drop the list of entries, in favour of a single element.
        # This simulates Google Workspace / GSuite activity logs being run through the
        # path splitter first.
        entry = entries[0]
        entry["events"] = entry["events"][2]

        # Process the entries.
        records = []
        records.extend(self.processor.process(entry))

        # Confirm fields are remapped into key / values after processing.
        record = records[0]
        self.assertEqual(record["events"]["parameters"]["owner_is_shared_drive"], False)
        self.assertEqual(record["events"]["parameters"]["visibility"], "private")
        self.assertEqual(record["events"]["parameters"]["new_value"], ["owner"])

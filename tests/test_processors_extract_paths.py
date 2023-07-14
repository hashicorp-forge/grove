# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the path extracting processor."""

import json
import os
import unittest
from unittest.mock import patch

from grove.models import ProcessorConfig
from grove.processors import extract_paths
from tests import mocks


class ProcessorPathExtratTestCase(unittest.TestCase):
    """Implements unit tests for the path extracting processor."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Setup the processor and associated configuration for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))

        # Create a mapping compatible with the Okta system log fixture.
        self.processor = extract_paths.Handler(
            ProcessorConfig(
                name="ecs",
                processor="extract_paths",
                raw="event.original",
                fields=[
                    {
                        "destination": "@timestamp",
                        "sources": [
                            "published",
                        ],
                    },
                    {
                        "destination": "'source.ip'",
                        "sources": [
                            "client.ipAddress",
                        ],
                    },
                    {
                        "destination": "'ecs.version'",
                        "static": "8.8",
                    },
                    {
                        "destination": "nested.key",
                        "static": "example",
                    },
                    {
                        "destination": "another.nested.key",
                        "sources": [
                            "client.device",
                        ],
                    },
                ],
            )
        )

    def test_extract_paths(self):
        """Ensure path extraction operates as expected."""
        # Load and process the fixture.
        entries = json.load(
            open(os.path.join(self.dir, "fixtures/okta/system_log/001.json"), "r")
        )

        # Process a single target.
        target = entries[0]
        results = self.processor.process(target)

        # Ensure fields are is mapped correctly.
        self.assertEqual(results[0]["source.ip"], "000.000.00.000")
        self.assertEqual(results[0]["@timestamp"], "2021-06-24T00:04:08.123Z")
        self.assertEqual(results[0]["ecs.version"], "8.8")
        self.assertEqual(results[0]["nested"]["key"], "example")
        self.assertEqual(results[0]["another"]["nested"]["key"], "Computer")

        # Ensure the raw message is present.
        self.assertGreater(len(results[0]["event"]["original"]), 0)

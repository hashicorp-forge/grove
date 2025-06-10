# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the GitHub Rulesets collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.github.rulesets import Connector
from grove.models import ConnectorConfig
from tests import mocks


class GitHubRulesetstTestCase(unittest.TestCase):
    """Implements unit tests for the GitHub Rulesets collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="examplecorp",
                key="ghp_c0ffeec0ffeec0ffeec0ffeec0ffee",
                name="examplecorp",
                connector="test",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_collect_no_pagination(self):
        """Ensure collection without pagination is working as expected."""
        responses.add(
            responses.GET,
            re.compile(r"https://.*/rule-suites"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/github/rulesets/002.json"), "r"
                ).read(),
                "utf-8",
            ),
        )
        responses.add(
            responses.GET,
            re.compile(r"https://.*/rule-suites/[0-9]+"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/github/rule-suite/001.json"), "r"
                ).read(),
                "utf-8",
            ),
        )

        # Ensure the correct number of value are returned, and the pointer properly set.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 1)
        self.assertEqual(self.connector.pointer, "2025-05-23T18:40:27+01:00")

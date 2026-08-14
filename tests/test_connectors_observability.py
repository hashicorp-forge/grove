# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for connector observability (error and zero-volume events)."""

import unittest
from unittest.mock import patch

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import RequestFailedException
from grove.models import ConnectorConfig, ObserverEventType
from tests import mocks
from tests.mocks.observer import TestHandler


class _FailingConnector(BaseConnector):
    """A connector whose collection always fails."""

    CONNECTOR = "failing"
    POINTER_PATH = "id"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        raise RequestFailedException("boom")


class _EmptyConnector(BaseConnector):
    """A connector which collects successfully but returns no data."""

    CONNECTOR = "empty"
    POINTER_PATH = "id"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        return


CONTEXT = {"runtime": "test_harness", "runtime_id": "NA"}


class ConnectorObservabilityTestCase(unittest.TestCase):
    """Implements tests for connector observability."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_error_emits_event(self):
        """Ensures an error event is emitted when collection fails."""
        connector = _FailingConnector(
            config=ConnectorConfig(
                key="token",
                name="test",
                identity="1FEEDFEED1",
                connector="failing",
            ),
            context=CONTEXT,
        )
        observer = TestHandler()
        connector._observer = observer

        connector.run()

        self.assertEqual(len(observer.events), 1)
        self.assertEqual(observer.events[0].event, ObserverEventType.error)
        self.assertIn("boom", observer.events[0].error)

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_zero_volume_threshold(self):
        """Ensures zero-volume events fire only after the configured threshold."""
        connector = _EmptyConnector(
            config=ConnectorConfig(
                key="token",
                name="test",
                identity="1FEEDFEED1",
                connector="empty",
                zero_volume_runs=2,
            ),
            context=CONTEXT,
        )
        observer = TestHandler()
        connector._observer = observer

        # First empty run - below threshold, no event.
        connector._check_zero_volume()
        self.assertEqual(len(observer.events), 0)

        # Second empty run - threshold reached, one event.
        connector._check_zero_volume()
        self.assertEqual(len(observer.events), 1)
        self.assertEqual(observer.events[0].event, ObserverEventType.zero_volume)

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_zero_volume_resets_on_data(self):
        """Ensures the consecutive counter resets when data is collected."""
        connector = _EmptyConnector(
            config=ConnectorConfig(
                key="token",
                name="test",
                identity="1FEEDFEED1",
                connector="empty",
                zero_volume_runs=2,
            ),
            context=CONTEXT,
        )
        observer = TestHandler()
        connector._observer = observer

        # One empty run, then a run with data resets the counter.
        connector._check_zero_volume()
        connector._saved["logs"] = 5
        connector._check_zero_volume()
        self.assertEqual(len(observer.events), 0)

        # Two further empty runs are required before the next event.
        connector._saved["logs"] = 0
        connector._check_zero_volume()
        self.assertEqual(len(observer.events), 0)

        connector._check_zero_volume()
        self.assertEqual(len(observer.events), 1)

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_zero_volume_disabled_by_default(self):
        """Ensures zero-volume alerting is disabled when not configured."""
        connector = _EmptyConnector(
            config=ConnectorConfig(
                key="token",
                name="test",
                identity="1FEEDFEED1",
                connector="empty",
            ),
            context=CONTEXT,
        )
        observer = TestHandler()
        connector._observer = observer

        for _ in range(5):
            connector._check_zero_volume()

        self.assertEqual(len(observer.events), 0)

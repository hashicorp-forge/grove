# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the base observer functionality."""

import os
import unittest

from grove.models import (
    ObserverEvent,
    ObserverEventSeverity,
    ObserverEventType,
)
from grove import observers
from tests.mocks.observer import RaisingHandler, TestHandler


def _event(event: ObserverEventType = ObserverEventType.error) -> ObserverEvent:
    """Builds a sample observability event for testing."""
    return ObserverEvent(
        event=event,
        severity=ObserverEventSeverity.warning,
        connector="grove.connectors.example",
        name="example",
        identity="examplecorp",
        operation="all",
    )


class ObserverBaseTestCase(unittest.TestCase):
    """Implements tests for the base observer functionality."""

    def tearDown(self):
        if "GROVE_OBSERVER_HANDLER" in os.environ:
            del os.environ["GROVE_OBSERVER_HANDLER"]

    def test_enabled_for_no_allowlist(self):
        """Ensures all event types are enabled when no allow-list is set."""
        handler = TestHandler()

        self.assertTrue(handler.enabled_for(ObserverEventType.error))
        self.assertTrue(handler.enabled_for(ObserverEventType.stale))
        self.assertTrue(handler.enabled_for(ObserverEventType.zero_volume))

    def test_enabled_for_allowlist(self):
        """Ensures only allow-listed event types are enabled."""
        handler = TestHandler()
        handler.config.events = "error, stale"

        self.assertTrue(handler.enabled_for(ObserverEventType.error))
        self.assertTrue(handler.enabled_for(ObserverEventType.stale))
        self.assertFalse(handler.enabled_for(ObserverEventType.zero_volume))

    def test_emit_skips_when_none(self):
        """Ensures emit is a no-op when no observer is configured."""
        # Should not raise.
        observers.emit(None, _event())

    def test_emit_records_event(self):
        """Ensures emit passes the event to the handler."""
        handler = TestHandler()

        observers.emit(handler, _event())

        self.assertEqual(len(handler.events), 1)
        self.assertEqual(handler.events[0].event, ObserverEventType.error)

    def test_emit_respects_allowlist(self):
        """Ensures emit filters events based on the handler allow-list."""
        handler = TestHandler()
        handler.config.events = "stale"

        observers.emit(handler, _event(ObserverEventType.error))
        self.assertEqual(len(handler.events), 0)

        observers.emit(handler, _event(ObserverEventType.stale))
        self.assertEqual(len(handler.events), 1)

    def test_emit_swallows_exceptions(self):
        """Ensures a failing observer does not raise to the caller."""
        handler = RaisingHandler()

        # Should not raise.
        observers.emit(handler, _event())

    def test_load_returns_none_when_unset(self):
        """Ensures load returns None when no handler is configured."""
        self.assertIsNone(observers.load())

    def test_load_returns_handler_when_set(self):
        """Ensures load returns a configured handler."""
        os.environ["GROVE_OBSERVER_HANDLER"] = "webhook"
        os.environ["GROVE_OBSERVER_WEBHOOK_URL"] = "https://192.0.2.1"

        try:
            observer = observers.load()
            self.assertIsNotNone(observer)
        finally:
            del os.environ["GROVE_OBSERVER_WEBHOOK_URL"]

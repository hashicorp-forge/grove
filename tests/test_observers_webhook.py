# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the webhook observer handler."""

import json
import os
import re
import unittest

import responses

from grove.exceptions import AccessException
from grove.models import (
    ObserverEvent,
    ObserverEventSeverity,
    ObserverEventType,
)
from grove.observers.webhook import Handler


def _event() -> ObserverEvent:
    """Builds a sample observability event for testing."""
    return ObserverEvent(
        event=ObserverEventType.error,
        severity=ObserverEventSeverity.critical,
        connector="grove.connectors.example",
        name="example",
        identity="examplecorp",
        operation="all",
        error="Something went wrong.",
    )


class WebhookObserverTestCase(unittest.TestCase):
    """Implements tests for the webhook observer handler."""

    def setUp(self):
        os.environ["GROVE_OBSERVER_WEBHOOK_URL"] = "https://192.0.2.1"
        os.environ["GROVE_OBSERVER_WEBHOOK_HEADERS"] = (
            "content-type: test/fixture|Example: My-Example"
        )
        os.environ["GROVE_OBSERVER_WEBHOOK_RETRIES"] = "3"

        self.handler = Handler()
        self.handler.setup()

    def tearDown(self):
        for var in [
            "GROVE_OBSERVER_WEBHOOK_URL",
            "GROVE_OBSERVER_WEBHOOK_HEADERS",
            "GROVE_OBSERVER_WEBHOOK_RETRIES",
        ]:
            if var in os.environ:
                del os.environ[var]

    def test_setup(self):
        """Ensures headers are correctly set."""
        EXPECTED_HEADERS = {
            "content-type": "test/fixture",
            "Example": "My-Example",
        }

        self.assertDictEqual(EXPECTED_HEADERS, self.handler._headers)

    @responses.activate
    def test_emit_success(self):
        """Ensures no errors on a successful POST, and the body is the event JSON."""
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(),
        )

        self.handler.emit(_event())

        self.assertEqual(len(responses.calls), 1)

        # Ensure the emitted body is the serialized event.
        body = json.loads(responses.calls[0].request.body)
        self.assertEqual(body["event"], "error")
        self.assertEqual(body["identity"], "examplecorp")
        self.assertEqual(body["error"], "Something went wrong.")

    @responses.activate
    def test_emit_retry_failure(self):
        """Ensures an AccessException is raised after the retry count is exceeded."""
        RETRIES = 3

        for _ in range(0, RETRIES):
            responses.add(
                responses.POST,
                re.compile(r"https://.*"),
                status=500,
                content_type="application/json",
                body=bytes(),
            )

        with self.assertRaises(AccessException):
            self.handler.emit(_event())

        self.assertEqual(len(responses.calls), RETRIES)

    @responses.activate
    def test_emit_after_retry(self):
        """Ensures no errors if a successful POST is seen after a retry."""
        WANTED_TRIES = 2

        for _ in range(0, WANTED_TRIES - 1):
            responses.add(
                responses.POST,
                re.compile(r"https://.*"),
                status=500,
                content_type="application/json",
                body=bytes(),
            )

        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(),
        )

        self.handler.emit(_event())

        self.assertEqual(len(responses.calls), WANTED_TRIES)

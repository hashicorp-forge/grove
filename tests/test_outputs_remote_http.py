# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the remote HTTP output handler."""

import os
import re
import unittest

import responses

from grove.exceptions import AccessException
from grove.outputs.remote_http import Handler


class RemoteHTTPOutputTestCase(unittest.TestCase):
    """Implements tests for the remote HTTP output handler."""

    def setUp(self):
        os.environ["GROVE_OUTPUT_REMOTE_HTTP_URL"] = "https://192.0.2.1"
        os.environ["GROVE_OUTPUT_REMOTE_HTTP_HEADERS"] = (
            "content-type: test/fixture|Example: My-Example"
        )

        self.handler = Handler()
        self.handler.setup()

    def test_setup(self):
        """Ensures headers are correctly set."""
        EXPECTED_HEADERS = {
            "content-type": "test/fixture",
            "Example": "My-Example",
        }

        # Ensure the headers are processed correctly.
        self.assertDictEqual(EXPECTED_HEADERS, self.handler._headers)

    @responses.activate
    def test_submit_retry_failure(self):
        """Ensures an error is raised after the maximum retry count is exceeded."""
        DEFAULT_RETRIES = 5

        # Simulate the default number of failures.
        for _ in range(0, DEFAULT_RETRIES):
            responses.add(
                responses.POST,
                re.compile(r"https://.*"),
                status=500,
                content_type="application/json",
                body=bytes(),
            )

        with self.assertRaises(AccessException):
            self.handler.submit(
                data=bytes("{}", "utf-8"),
                connector="X",
                identity="Y",
                operation="Z",
            )

        # Ensure we only attempted the configured number of times.
        self.assertEqual(len(responses.calls), DEFAULT_RETRIES)

    @responses.activate
    def test_submit_success(self):
        """Ensures no errors on successful POST."""
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(),
        )

        self.handler.submit(
            data=bytes("{}", "utf-8"),
            connector="X",
            identity="Y",
            operation="Z",
        )

        # Ensure we submitted on the first attempt.
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_submit_after_retry(self):
        """Ensures no errors if a successful POST is seen after a retry."""
        WANTED_TRIES = 3

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

        self.handler.submit(
            data=bytes("{}", "utf-8"),
            connector="X",
            identity="Y",
            operation="Z",
        )

        self.assertEqual(len(responses.calls), WANTED_TRIES)

    def test_serialize(self):
        """Ensures serialization into NDJSON is functional."""
        candidate = [
            {"id": "0001", "name": "One"},
            {"id": "0002", "name": "Two"},
            {"id": "0003", "name": "Three"},
        ]

        # Manually constructed.
        expected_raw = "\r\n".join(
            [
                '{"id":"0001","name":"One","_grove":{"field":"value"}}',
                '{"id":"0002","name":"Two","_grove":{"field":"value"}}',
                '{"id":"0003","name":"Three","_grove":{"field":"value"}}',
            ]
        )
        expected_raw = bytes(expected_raw, "utf-8")

        self.assertEqual(
            expected_raw,
            self.handler.serialize(data=candidate, metadata={"field": "value"}),
        )

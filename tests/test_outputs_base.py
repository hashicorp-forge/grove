# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the base output handler."""

import gzip
import unittest

from grove.outputs import BaseOutput


# Required as BaseOutput is an ABC, so without defining submit we will not be able to
# instantiate it to validate methods on the base class.
class TestOutput(BaseOutput):
    __test__ = False

    def submit(self, *args, **kwargs):
        pass


class BaseOutputTestCase(unittest.TestCase):
    """Implements tests for the base output handler."""

    def test_serialize(self):
        """Ensures serialization into NDJSON is functional."""
        handler = TestOutput()

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
        expected_gzip = gzip.compress(bytes(expected_raw, "utf-8"))

        self.assertEqual(
            expected_gzip,
            handler.serialize(data=candidate, metadata={"field": "value"}),
        )

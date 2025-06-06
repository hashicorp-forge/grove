# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Google BigQuery utility functions."""

import unittest
from datetime import datetime, timezone

from grove.connectors.google.utils import (
    as_bigquery_timestamp_microseconds,
    as_bigquery_timestamp_seconds,
    as_bigquery_timestamp_milliseconds,
)


class GoogleBigQueryUtilsTestCase(unittest.TestCase):
    """Implements unit tests for the Google BigQuery utility functions."""

    def test_as_bigquery_timestamp_microseconds(self):
        """Ensure microsecond timestamp conversion works as expected."""
        # Test with integer microseconds (Gmail logs format)
        us_timestamp = 1738500089504000  # 2025-02-02 12:41:29.504 UTC
        result = as_bigquery_timestamp_microseconds(us_timestamp)
        self.assertEqual(result, "2025-02-02 12:41:29+00")

        # Test with string input
        us_timestamp_str = "1738500089504000"
        result = as_bigquery_timestamp_microseconds(us_timestamp_str)
        self.assertEqual(result, "2025-02-02 12:41:29+00")

        # Test with float input
        us_timestamp_float = 1738500089504000.0
        result = as_bigquery_timestamp_microseconds(us_timestamp_float)
        self.assertEqual(result, "2025-02-02 12:41:29+00")

    def test_as_bigquery_timestamp_seconds(self):
        """Ensure second timestamp conversion works as expected."""
        # Test with integer seconds (Unix timestamp)
        sec_timestamp = 1738500089  # 2025-02-02 12:41:29 UTC
        result = as_bigquery_timestamp_seconds(sec_timestamp)
        self.assertEqual(result, "2025-02-02 12:41:29+00")

        # Test with string input
        sec_timestamp_str = "1738500089"
        result = as_bigquery_timestamp_seconds(sec_timestamp_str)
        self.assertEqual(result, "2025-02-02 12:41:29+00")

        # Test with float input (for fractional seconds)
        sec_timestamp_float = 1738500089.5
        result = as_bigquery_timestamp_seconds(sec_timestamp_float)
        self.assertEqual(result, "2025-02-02 12:41:29+00")

    def test_as_bigquery_timestamp_milliseconds(self):
        """Ensure millisecond timestamp conversion works as expected."""
        # Test with integer milliseconds (JavaScript Date.now() format)
        ms_timestamp = 1738500089504  # 2025-02-02 12:41:29.504 UTC
        result = as_bigquery_timestamp_milliseconds(ms_timestamp)
        self.assertEqual(result, "2025-02-02 12:41:29+00")

        # Test with string input
        ms_timestamp_str = "1738500089504"
        result = as_bigquery_timestamp_milliseconds(ms_timestamp_str)
        self.assertEqual(result, "2025-02-02 12:41:29+00")

        # Test with float input
        ms_timestamp_float = 1738500089504.0
        result = as_bigquery_timestamp_milliseconds(ms_timestamp_float)
        self.assertEqual(result, "2025-02-02 12:41:29+00")

    def test_timezone_handling(self):
        """Ensure all functions handle timezone correctly."""
        # All functions should produce UTC timestamps with +00 suffix
        us_timestamp = 1738500089504000
        sec_timestamp = 1738500089
        ms_timestamp = 1738500089504

        us_result = as_bigquery_timestamp_microseconds(us_timestamp)
        sec_result = as_bigquery_timestamp_seconds(sec_timestamp)
        ms_result = as_bigquery_timestamp_milliseconds(ms_timestamp)

        # All should end with +00 (UTC)
        self.assertTrue(us_result.endswith("+00"))
        self.assertTrue(sec_result.endswith("+00"))
        self.assertTrue(ms_result.endswith("+00"))

        # Microseconds and milliseconds should produce the same result for this timestamp
        self.assertEqual(us_result, ms_result)

    def test_format_consistency(self):
        """Ensure all functions produce consistent BigQuery timestamp format."""
        us_timestamp = 1738500089504000
        sec_timestamp = 1738500089
        ms_timestamp = 1738500089504

        us_result = as_bigquery_timestamp_microseconds(us_timestamp)
        sec_result = as_bigquery_timestamp_seconds(sec_timestamp)
        ms_result = as_bigquery_timestamp_milliseconds(ms_timestamp)

        # All should match the expected format: YYYY-MM-DD HH:MM:SS+00
        import re
        pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+00$'
        
        self.assertTrue(re.match(pattern, us_result))
        self.assertTrue(re.match(pattern, sec_result))
        self.assertTrue(re.match(pattern, ms_result))

    def test_epoch_edge_cases(self):
        """Test edge cases around epoch time."""
        # Test epoch start (1970-01-01 00:00:00 UTC)
        epoch_us = 0
        epoch_sec = 0
        epoch_ms = 0

        us_result = as_bigquery_timestamp_microseconds(epoch_us)
        sec_result = as_bigquery_timestamp_seconds(epoch_sec)
        ms_result = as_bigquery_timestamp_milliseconds(epoch_ms)

        self.assertEqual(us_result, "1970-01-01 00:00:00+00")
        self.assertEqual(sec_result, "1970-01-01 00:00:00+00")
        self.assertEqual(ms_result, "1970-01-01 00:00:00+00") 
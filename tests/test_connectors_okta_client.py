# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Okta API Client."""

import unittest

from grove.connectors.okta.api import Client


class OktaAuditTestCase(unittest.TestCase):
    """Implements unit tests for the Okta API Client."""

    def test_client_parse_link_header(self):
        """Ensure Link header parsing works as expected."""
        client = Client(identity="example", token="Key")

        # Ensure the next page URL is extracted correctly.
        candidate = (
            '<https://example.okta.com/api/v1/logs?since=2021&limit=1000&sortOrder=ASCENDING&after=1625253400145_1>; rel="next", '
            '<https://example.okta.com/api/v1/logs?since=2021&limit=1000&sortOrder=ASCENDING&after=1625251850961_1>; rel="self"'
        )
        expected = "https://example.okta.com/api/v1/logs?since=2021&limit=1000&sortOrder=ASCENDING&after=1625253400145_1"
        self.assertEqual(client._parse_link_header(candidate), expected)

        # Ensure its the last page
        candidate = (
            '<https://example.okta.com/api/v1/logs?since=2021&limit=1000&sortOrder=ASCENDING&after=1625253400145_1>; rel="next", '
            '<https://example.okta.com/api/v1/logs?since=2021&limit=1000&sortOrder=ASCENDING&after=1625253400145_1>; rel="self"'
        )
        with self.assertRaises(ValueError):
            client._parse_link_header(candidate)

        # Ensure SSRF mitigation is working correctly.
        candidate = (
            '<https://192.0.2.1:8080/api/v1/logs?since=2021&limit=1000&sortOrder=ASCENDING&after=1625253400145_1>; rel="next", '
            '<https://example.okta.com/api/v1/logs?since=2021&limit=1000&sortOrder=ASCENDING&after=1625251850961_1>; rel="self"'
        )
        with self.assertRaises(ValueError):
            client._parse_link_header(candidate)

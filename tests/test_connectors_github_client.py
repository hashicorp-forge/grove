# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the GitHub API Client."""

import unittest

from grove.connectors.github.api import Client


class GitHubClientTestCase(unittest.TestCase):
    """Implements tests for the GitHub API Client."""

    def test_client_parse_link_header(self):
        """Ensure Link header parsing works as expected."""
        client = Client(identity="Identity", token="Key")

        # Ensure the next page URL is extracted correctly.
        candidate = (
            '<https://api.github.com/user/repos?page=3&per_page=100>; rel="next", '
            '<https://api.github.com/user/repos?page=50&per_page=100>; rel="last"'
        )
        expected = "https://api.github.com/user/repos?page=3&per_page=100"
        self.assertEqual(client._parse_link_header(candidate), expected)

        # Ensure SSRF mitigation is working correctly.
        candidate = (
            '<http://192.0.2.1:8080/some/vulnerable/service?action=thing>; rel="next", '
            '<https://api.github.com/user/repos?page=50&per_page=100>; rel="last"'
        )
        with self.assertRaises(ValueError):
            client._parse_link_header(candidate)

        # Ensure a ValueError is returned when there are no pages left.
        candidate = (
            '<https://api.github.com/user/repos?page=3&per_page=100>; rel="first", '
            '<https://api.github.com/user/repos?page=50&per_page=100>; rel="previous"'
        )
        with self.assertRaises(ValueError):
            client._parse_link_header(candidate)

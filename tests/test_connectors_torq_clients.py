# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Torq API Client."""

import base64
import os
import re
import unittest
from unittest.mock import patch

import responses
from responses import matchers

from grove.connectors.torq.api import Client
from tests import mocks


class TorqClientTestCases(unittest.TestCase):
    """Implements unit tests for the Torq API Client."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.identity = "CLIENT_ID"
        self.key = "CLIENT_SECRET"
        self.client = None

    @responses.activate
    def test_get_bearer_token(self):
        """Ensure we get bearer token from client-id client-secret."""
        set_expected_basic_auth = str(
            base64.b64encode(bytes(f"{self.identity}:{self.key}", "utf-8")),
            "utf-8",
        )
        # auth response
        responses.add(
            responses.POST,
            re.compile(r"https://.*"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/torq/client/001.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
            match=[
                matchers.header_matcher(
                    {"Authorization": f"Basic {set_expected_basic_auth}"}
                )
            ],
        )
        # token hardcoded from fixtures file
        resp = {"access_token": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
        self.assertIsNone(self.client)
        self.client = Client(identity=self.identity, key=self.key)
        self.assertEqual(resp.get("access_token"), self.client.bearer_token)

        # make sure the authorization header is set with the new bearer token
        self.assertEqual(
            f"Bearer {resp.get('access_token')}",
            self.client.headers.get("Authorization"),
        )

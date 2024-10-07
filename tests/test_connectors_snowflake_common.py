# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the Snowflake record collector."""

import base64
import os
import unittest
from unittest.mock import patch

from grove.connectors.snowflake.common import SnowflakeConnector
from grove.exceptions import ConfigurationException
from grove.models import ConnectorConfig
from tests import mocks


class SnowflakeAuditTestCase(unittest.TestCase):
    """Implements tests for the Snowflake record collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))

    def test_load_unencrypted_key(self):
        """Ensures unencrypted private key loading is working as expected."""
        # Unencrypted.
        key = os.path.join(self.dir, "fixtures/snowflake/pkcs8.b64")

        # The unencrypted key should load without error.
        candidate = SnowflakeConnector(
            config=ConnectorConfig(
                identity="1FEEDFEED1",
                name="test",
                connector="test",
                key=open(key, "rb").read(),
                encoding={
                    "key": "base64",
                },
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        _ = candidate._load_private_key()

    def test_load_encrypted_key(self):
        """Ensures encrypted private key loading is working as expected."""
        # Encrypted with passphrase 'example'.
        key = os.path.join(self.dir, "fixtures/snowflake/pkcs8_enc.b64")

        # The encrypted key should fail to load without a passphrase.
        candidate = SnowflakeConnector(
            config=ConnectorConfig(
                identity="1FEEDFEED1",
                name="test",
                connector="test",
                key=open(key, "rb").read(),
                encoding={
                    "key": "base64",
                },
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        with self.assertRaises(ConfigurationException):
            _ = candidate._load_private_key()

        # Ensure loading when the passphrase is correctly set.
        candidate.configuration.passphrase = "example"
        candidate._load_private_key()

    def test_field_account(self):
        """Ensures that the account configuration field is required."""
        with self.assertRaises(ConfigurationException):
            candidate = SnowflakeConnector(
                config=ConnectorConfig(
                    identity="1FEEDFEED1",
                    key="",
                    name="test",
                    connector="test",
                ),
                context={
                    "runtime": "test_harness",
                    "runtime_id": "NA",
                },
            )
            _ = candidate.account

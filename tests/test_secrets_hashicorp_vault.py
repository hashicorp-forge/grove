# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the HashiCorp Vault secrets backend."""

import json
import os
import re
import tempfile
import unittest

import responses

from grove.exceptions import AccessException
from grove.secrets.hashicorp_vault import Handler


class SecretsHashiCorpVaultTestCase(unittest.TestCase):
    """Implements tests for the HashiCorp Vault secrets backend."""

    @responses.activate
    def setUp(self):
        self.token = "A_VERY_SECRET_VALUE"

        # Set sensible defaults for the test harness.
        os.environ["GROVE_SECRET_HASHICORP_VAULT_ADDR"] = "http://192.0.2.1:8200"
        os.environ["GROVE_SECRET_HASHICORP_VAULT_TOKEN"] = self.token

        # Mock the pre-flight to always succeed (used to validate credentials).
        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=200,
            content_type="application/json",
            body=bytes(),
        )
        self.secrets = Handler()

    @responses.activate
    def test_token_from_file(self):
        """Ensures Vault token can be read from file."""
        expected = "THIS_IS_A_SECRET_FROM_FILE"

        # Mock the pre-flight to always succeed (used to validate credentials).
        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=200,
            content_type="application/json",
            body=bytes(),
        )

        with tempfile.NamedTemporaryFile("w") as fout:
            fout.write(expected)
            fout.write("\n")
            fout.flush()

            # Setup a new Vault handler using this file.
            os.environ["GROVE_SECRET_HASHICORP_VAULT_TOKEN_FILE"] = fout.name

            secrets = Handler()

        # Check that the token on the configured / setup secrets handler matches the
        # value written to the temporary file.
        self.assertEqual(secrets.config.token, expected)

    @responses.activate
    def test_client_setup(self):
        """Ensure the client validates credentials on setup."""
        # Pre-flight should fail if Vault is not reachable (no mock, and TEST-NET-1 URL)
        with self.assertRaises(AccessException):
            _ = Handler()

        # Mock a successful pre-flight - used to validate credentials
        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=200,
            content_type="application/json",
            body=bytes(),
        )
        secrets = Handler()

        # Ensure that the token on the handler matches the expected value.
        secrets.config.token = self.token

        # Mock an unsuccessful pre-flight.
        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=401,
            content_type="application/json",
            body=bytes(),
        )
        with self.assertRaises(AccessException):
            _ = Handler()

    @responses.activate
    def test_get_field_and_path(self):
        """Ensure that field and path extraction is working as expected."""
        # Ensure fields are extracted correctly.
        candidate = "secret/data/example/demo?field=password"
        field, path = self.secrets.get_field_and_path(candidate)

        self.assertEqual(field, "password")
        self.assertEqual(path, "secret/data/example/demo")

        # Ensure omitted fields raise an exception.
        candidate = "secret/data/example/demo"
        with self.assertRaises(ValueError):
            self.secrets.get_field_and_path(candidate)

    @responses.activate
    def test_get_field_v1(self):
        """Ensure that a KVv1 secret can be fetched."""
        candidate = "secret/example/demo?field=password"
        expected = "SUPER_SECRET_VALUE"

        # Positive case - field and secret exists.
        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=200,
            content_type="application/json",
            body=bytes(json.dumps({"data": {"password": expected}}), "utf-8"),
        )

        secret = self.secrets.get(candidate)
        self.assertEqual(secret, expected)

        # Negative case - incorrect / missing field should raise an error.
        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=200,
            content_type="application/json",
            body=bytes(json.dumps({"data": {"unknown": expected}}), "utf-8"),
        )

        with self.assertRaises(AccessException):
            _ = self.secrets.get(candidate)

    @responses.activate
    def test_get_error(self):
        """Ensure authentication errors are handled."""
        candidate = "secret/data/example/demo?field=token"

        # Negative case - incorrect / missing field should raise an error.
        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=401,
            content_type="application/json",
            body=bytes(),
        )

        with self.assertRaises(AccessException):
            _ = self.secrets.get(candidate)

    @responses.activate
    def test_get_field_v2(self):
        """Ensure that a KVv2 secret can be fetched."""
        candidate = "secret/data/example/demo?field=token"
        expected = "SUPER_SECRET_VALUE_TWO"

        # Positive case - field and secret exists.
        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=200,
            content_type="application/json",
            body=bytes(json.dumps({"data": {"data": {"token": expected}}}), "utf-8"),
        )

        secret = self.secrets.get(candidate)
        self.assertEqual(secret, expected)

        # Negative case - incorrect / missing field should raise an error.
        responses.add(
            responses.GET,
            re.compile(r"http://.*"),
            status=200,
            content_type="application/json",
            body=bytes(json.dumps({"data": {"data": {"unknown": expected}}}), "utf-8"),
        )

        with self.assertRaises(AccessException):
            _ = self.secrets.get(candidate)

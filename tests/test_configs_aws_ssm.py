# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the configuration helpers."""

import json
import unittest

import boto3
from moto import mock_ssm

from grove.configs import aws_ssm
from grove.models import ConnectorConfig


class ConfigsAWSSSMTestCase(unittest.TestCase):
    """Implements tests for the Base connector."""

    @mock_ssm
    def test_get_valid_configuration(self):
        """Ensures that the SSM get method is working as expected."""
        fixtures = {
            "/grove/connectors/example/something": {
                "name": "test_one",
                "key": "SUPER_SECRET_ONE",
                "identity": "AAAAAAAA",
                "connector": "first_connector",
            },
            "/grove/connectors/example/some_other_thing": {
                "name": "test_two",
                "key": "SUPER_SECRET_TWO",
                "identity": "BBBBBBBB",
                "connector": "second_connector",
            },
        }

        expected = [
            ConnectorConfig(
                name="test_one",
                key="SUPER_SECRET_ONE",
                identity="AAAAAAAA",
                connector="first_connector",
            ),
            ConnectorConfig(
                name="test_two",
                key="SUPER_SECRET_TWO",
                identity="BBBBBBBB",
                connector="second_connector",
            ),
        ]

        # Push fixtures into the mock SSM.
        client = boto3.client("ssm", region_name="us-east-1")

        for name, fixture in fixtures.items():
            client.put_parameter(
                Name=name, Value=json.dumps(fixture), Type="SecureString"
            )

        # Fetch connector configuration documents via our handler, and ensure they are
        # as we expect.
        configurations = aws_ssm.Handler().get("/")

        for index, configuration in enumerate(configurations):
            self.assertEqual(expected[index], configuration)

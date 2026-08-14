# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the AWS SNS observer handler."""

import os
import unittest

import boto3
from moto import mock_sns

from grove.exceptions import ConfigurationException
from grove.models import (
    ObserverEvent,
    ObserverEventSeverity,
    ObserverEventType,
)
from grove.observers.aws_sns import Handler


def _event() -> ObserverEvent:
    """Builds a sample observability event for testing."""
    return ObserverEvent(
        event=ObserverEventType.stale,
        severity=ObserverEventSeverity.warning,
        connector="grove.connectors.example",
        name="example",
        identity="examplecorp",
        operation="all",
    )


class SNSObserverTestCase(unittest.TestCase):
    """Implements tests for the AWS SNS observer handler."""

    def tearDown(self):
        for var in [
            "GROVE_OBSERVER_AWS_SNS_TOPIC_ARN",
            "GROVE_OBSERVER_AWS_SNS_REGION",
        ]:
            if var in os.environ:
                del os.environ[var]

    @mock_sns
    def test_emit_success(self):
        """Ensures an event can be published to SNS without error."""
        client = boto3.client("sns", region_name="us-east-1")
        topic = client.create_topic(Name="grove-observability")

        os.environ["GROVE_OBSERVER_AWS_SNS_TOPIC_ARN"] = topic["TopicArn"]
        os.environ["GROVE_OBSERVER_AWS_SNS_REGION"] = "us-east-1"

        handler = Handler()
        handler.setup()

        # Should not raise.
        handler.emit(_event())

    def test_missing_topic_arn(self):
        """Ensures a ConfigurationException is raised when no topic ARN is set."""
        if "GROVE_OBSERVER_AWS_SNS_TOPIC_ARN" in os.environ:
            del os.environ["GROVE_OBSERVER_AWS_SNS_TOPIC_ARN"]

        with self.assertRaises(ConfigurationException):
            Handler()

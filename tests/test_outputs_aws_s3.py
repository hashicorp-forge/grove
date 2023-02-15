# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the S3 output handler."""

import os
import unittest

import boto3
from moto import mock_s3

from grove.exceptions import ConfigurationException
from grove.outputs.aws_s3 import Handler


class S3OutputTestCase(unittest.TestCase):
    """Implements tests for the S3 output handler."""

    @mock_s3
    def setUp(self):
        self.client = boto3.resource("s3", region_name="us-east-1")
        self.client.create_bucket(Bucket="PersistentBucket")

        os.environ["GROVE_OUTPUT_AWS_S3_BUCKET"] = "PersistentBucket"

    @mock_s3
    def test_bucket_check(self):
        """Ensures the bucket location constraint check operates properly."""
        # Ensure the handler is setup when a properly configured bucket is specified.
        Handler()

        # Ensure that an exception is raised when no bucket is specified.
        if "GROVE_OUTPUT_AWS_S3_BUCKET" in os.environ:
            del os.environ["GROVE_OUTPUT_AWS_S3_BUCKET"]

        with self.assertRaises(ConfigurationException):
            Handler()

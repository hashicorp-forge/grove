# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove AWS S3 output handler."""

import datetime
import logging
import os
from typing import Optional

from boto3.session import Session
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseSettings, Field, ValidationError

from grove.constants import DATESTAMP_FORMAT
from grove.exceptions import AccessException, ConfigurationException
from grove.helpers import parsing
from grove.outputs import BaseOutput

OBJECT_KEY = (
    "logs/{connector}/{identity}/{year}/{month}/{day}/"
    "{operation}-{datestamp}.{part}.json.gz"
)


class Configuration(BaseSettings):
    """Defines environment variables used to configure the AWS S3 handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    bucket: str = Field(
        description="The name of the S3 bucket to output logs to.",
    )
    assume_role_arn: Optional[str] = Field(
        description="An optional AWS role to assume when authenticating with AWS."
    )
    bucket_region: Optional[str] = Field(
        description="The region that S3 the bucket exists in (default us-east-1)",
        default=os.environ.get("AWS_REGION", "us-east-1"),
    )

    class Config:
        """Allow environment variable override of configuration fields.

        This also enforce a prefix for all environment variables for this handler. As
        an example the field `bucket` would be set using the environment variable
        `GROVE_OUTPUT_AWS_S3_BUCKET`.
        """

        env_prefix = "GROVE_OUTPUT_AWS_S3_"
        case_insensitive = True


class Handler(BaseOutput):
    """This output handler allows Grove to write collected logs to an AWS S3 bucket."""

    def __init__(self):
        """Sets up access to S3.

        This handler also attempt to assume a configured role in order to allow
        cross-account use - if required.

        :raises ConfigurationException: There was an issue with configuration.
        :raises AccessException: An issue occurred when accessing S3.
        """
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = Configuration()
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

        # Explicit calls to session are mostly used to allow mocks during testing.
        session = Session()

        # If a role was specified, ensure we assume it and use STS tokens to interact
        # with S3.
        try:
            if not self.config.assume_role_arn:
                self.s3 = session.client("s3", region_name=self.config.bucket_region)
            else:
                sts = session.client("sts")
                role = sts.assume_role(
                    RoleArn=self.config.assume_role_arn,
                    RoleSessionName="GroveOutputWriter",
                )
                self.s3 = session.client(
                    "s3",
                    region_name=self.config.bucket_region,
                    aws_access_key_id=role["Credentials"]["AccessKeyId"],
                    aws_secret_access_key=role["Credentials"]["SecretAccessKey"],
                    aws_session_token=role["Credentials"]["SessionToken"],
                )
        except (ClientError, BotoCoreError) as err:
            raise AccessException(f"Output handler is unable to access AWS S3: {err}")

    def submit(
        self,
        data: bytes,
        connector: str,
        identity: str,
        operation: str,
        part: int = 0,
    ):
        """Persists captured data to an S3 compatible object store.

        :param data: Log data to write to S3.
        :param connector: Name of the connector which retrieved the data.
        :param identity: Identity the collected data was collect for.
        :param operation: Operation the collected logs are associated with.
        :param part: Number indicating which part of the same log stream this file
            contains data for. This is used to indicate that the logs are from the same
            collection, but have been broken into smaller files for downstream
            processing.

        :raises AccessException: An issue occurred when accessing S3.
        """
        try:
            datestamp = datetime.datetime.utcnow()
            self.s3.put_object(
                Body=data,
                Bucket=self.config.bucket,
                Key=OBJECT_KEY.format(
                    part=part,
                    connector=connector,
                    identity=identity,
                    operation=operation,
                    year=datestamp.strftime("%Y"),
                    month=datestamp.strftime("%m"),
                    day=datestamp.strftime("%d"),
                    datestamp=datestamp.strftime(DATESTAMP_FORMAT),
                ),
            )
        except ClientError as err:
            raise AccessException(f"Unable to write object to AWS S3: {err}")

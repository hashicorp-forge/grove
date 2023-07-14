# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove AWS S3 output handler."""

import datetime
import os
from typing import Optional

from boto3.session import Session
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import Field

from grove.constants import DATESTAMP_FORMAT
from grove.exceptions import AccessException
from grove.outputs import BaseOutput

OBJECT_KEY = (
    "{descriptor}{connector}/{identity}/{year}/{month}/{day}/"
    "{operation}-{datestamp}.{part}{kind}"
)


class Handler(BaseOutput):
    """This output handler allows Grove to write collected logs to an AWS S3 bucket."""

    class Configuration(BaseOutput.Configuration):
        """Defines environment variables used to configure the AWS S3 handler.

        This should also include any appropriate default values for fields which are not
        required.
        """

        bucket: str = Field(
            description="The name of the S3 bucket to output logs to.",
        )
        aws_access_key_id: Optional[str] = Field(
            description="An optional AWS access key to use when authenticating",
            default=os.environ.get("AWS_ACCESS_KEY_ID"),
        )
        aws_secret_access_key: Optional[str] = Field(
            description="An optional AWS secret key to use when authenticating",
            default=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
        aws_session_token: Optional[str] = Field(
            description="An optional AWS session token to use when authenticating",
            default=os.environ.get("AWS_SESSION_TOKEN"),
        )
        assume_role_arn: Optional[str] = Field(
            description="An optional AWS role to assume when authenticating with AWS.",
            default=None,
        )
        bucket_region: Optional[str] = Field(
            description="The region that S3 the bucket exists in (default us-east-1)",
            default=os.environ.get("AWS_REGION", "us-east-1"),
        )

        class Config:
            """Allow environment variable override of configuration fields.

            This also enforce a prefix for all environment variables for this handler.
            As an example the field `bucket` would be set using the environment variable
            `GROVE_OUTPUT_AWS_S3_BUCKET`.
            """

            env_prefix = "GROVE_OUTPUT_AWS_S3_"
            case_insensitive = True

    def setup(self):
        """Sets up access to S3.

        This handler also attempt to assume a configured role in order to allow
        cross-account use - if required.

        :raises ConfigurationException: There was an issue with configuration.
        :raises AccessException: An issue occurred when accessing S3.
        """
        # Explicit calls to session are mostly used to allow mocks during testing.
        session = Session()

        # Only add in optional arguments if configured.
        client_kwargs = {}

        if self.config.aws_access_key_id:
            client_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
            client_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

        if self.config.aws_session_token:
            client_kwargs["aws_session_token"] = self.config.aws_session_token

        # If a role was specified, ensure we assume it and use STS tokens to interact
        # with S3.
        try:
            if not self.config.assume_role_arn:
                self.s3 = session.client(
                    "s3",
                    region_name=self.config.bucket_region,
                    **client_kwargs,
                )
            else:
                sts = session.client(
                    "sts",
                    region_name=self.config.bucket_region,
                    **client_kwargs,
                )
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
        kind: Optional[str] = ".json.gz",
        descriptor: Optional[str] = "logs/",
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
        :param kind: An optional file suffix to use for objects written.
        :param descriptor: An optional path to append to the beginning of the output
            S3 key.

        :raises AccessException: An issue occurred when accessing S3.
        """
        # Append a trailing slash to the descriptor if set - to form a path.
        if descriptor and not descriptor.endswith("/"):
            descriptor = f"{descriptor}/"

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
                    descriptor=descriptor,
                    kind=kind,
                ),
            )
        except ClientError as err:
            raise AccessException(f"Unable to write object to AWS S3: {err}")

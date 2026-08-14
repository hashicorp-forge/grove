# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove AWS SNS observer handler."""

import os

from boto3.session import Session
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import Field

from grove.exceptions import AccessException
from grove.models import ObserverEvent
from grove.observers import BaseObserver

# The maximum length of an SNS 'Subject', per the AWS SNS API.
SUBJECT_MAX_LENGTH = 100


class Handler(BaseObserver):
    """This observer handler emits events to an AWS SNS topic."""

    class Configuration(BaseObserver.Configuration):
        """Defines environment variables used to configure the AWS SNS handler.

        This should also include any appropriate default values for fields which are not
        required.
        """

        topic_arn: str = Field(
            description="The ARN of the SNS topic to publish observability events to.",
        )
        aws_access_key_id: str | None = Field(
            description="An optional AWS access key to use when authenticating",
            default=os.environ.get("AWS_ACCESS_KEY_ID"),
        )
        aws_secret_access_key: str | None = Field(
            description="An optional AWS secret key to use when authenticating",
            default=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
        aws_session_token: str | None = Field(
            description="An optional AWS session token to use when authenticating",
            default=os.environ.get("AWS_SESSION_TOKEN"),
        )
        assume_role_arn: str | None = Field(
            description="An optional AWS role to assume when authenticating with AWS.",
            default=None,
        )
        region: str | None = Field(
            description="The region that the SNS topic exists in (default us-east-1)",
            default=os.environ.get("AWS_REGION", "us-east-1"),
        )

        class Config:
            """Allow environment variable override of configuration fields.

            This also enforces a prefix for all environment variables for this handler.
            As an example the field `topic_arn` would be set using the environment
            variable `GROVE_OBSERVER_AWS_SNS_TOPIC_ARN`.
            """

            env_prefix = "GROVE_OBSERVER_AWS_SNS_"
            case_insensitive = True

    def setup(self):
        """Sets up access to SNS.

        This handler also attempts to assume a configured role in order to allow
        cross-account use - if required.

        :raises ConfigurationException: There was an issue with configuration.
        :raises AccessException: An issue occurred when accessing SNS.
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
        # with SNS.
        try:
            if not self.config.assume_role_arn:
                self.sns = session.client(
                    "sns",
                    region_name=self.config.region,
                    **client_kwargs,
                )
            else:
                sts = session.client(
                    "sts",
                    region_name=self.config.region,
                    **client_kwargs,
                )
                role = sts.assume_role(
                    RoleArn=self.config.assume_role_arn,
                    RoleSessionName="GroveObserver",
                )
                self.sns = session.client(
                    "sns",
                    region_name=self.config.region,
                    aws_access_key_id=role["Credentials"]["AccessKeyId"],
                    aws_secret_access_key=role["Credentials"]["SecretAccessKey"],
                    aws_session_token=role["Credentials"]["SessionToken"],
                )
        except (ClientError, BotoCoreError) as err:
            raise AccessException(f"Observer is unable to access AWS SNS: {err}")

    def emit(self, event: ObserverEvent):
        """Publishes the event to the configured SNS topic.

        Message attributes are included to allow consumers to configure SNS subscription
        filter policies based on the event type, severity, and connector.

        :param event: The observability event to emit.

        :raises AccessException: An issue occurred when publishing to SNS.
        """
        subject = f"Grove {event.event.value}: {event.name} ({event.identity})"

        try:
            self.sns.publish(
                TopicArn=self.config.topic_arn,
                Subject=subject[:SUBJECT_MAX_LENGTH],
                Message=event.json(),
                MessageAttributes={
                    "event": {
                        "DataType": "String",
                        "StringValue": event.event.value,
                    },
                    "severity": {
                        "DataType": "String",
                        "StringValue": event.severity.value,
                    },
                    "connector": {
                        "DataType": "String",
                        "StringValue": event.connector,
                    },
                },
            )
        except (ClientError, BotoCoreError) as err:
            raise AccessException(
                f"Unable to publish observability event to AWS SNS: {err}"
            )

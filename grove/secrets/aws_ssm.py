# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove AWS SSM parameter store secret handler."""

import logging
import os
from typing import Optional

import jmespath
from boto3.session import Session
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseSettings, Field, ValidationError

from grove.exceptions import AccessException, ConfigurationException
from grove.helpers import parsing
from grove.secrets import BaseSecret


class Configuration(BaseSettings):
    """Defines environment variables used to configure the AWS SSM handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    assume_role_arn: Optional[str] = Field(
        description="An optional AWS role to assume when authenticating with AWS.",
        default=None,
    )
    ssm_region: Optional[str] = Field(
        description="The region that the parameter store exists in (default us-east-1)",
        default=os.environ.get("AWS_REGION", "us-east-1"),
    )

    class Config:
        """Allow environment variable override of configuration fields.

        This also enforce a prefix for all environment variables for this handler. As
        an example the field `assume_role_arn` would be set using the environment
        variable `GROVE_SECRET_AWS_SSM_ASSUME_ROLE_ARN`.
        """

        env_prefix = "GROVE_SECRET_AWS_SSM_"
        case_insensitive = True


class Handler(BaseSecret):
    """A configuration handler to read secrets from AWS SSM."""

    def __init__(self):
        """Sets up access to AWS SSM.

        This handler also attempt to assume a configured role in order to allow
        cross-account use - if required.

        :raises ConfigurationException: There was an issue with configuration.
        :raises AccessException: An issue occurred when accessing SSM.
        """
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = Configuration()
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

        # Explicit calls to session are mostly used to allow mocks during testing.
        session = Session()

        try:
            if not self.config.assume_role_arn:
                self._ssm = session.client("ssm", region_name=self.config.ssm_region)
            else:
                sts = session.client("sts")
                self.logger.debug(
                    "Secrets handler is attempting to assume AWS role for SSM",
                    extra={"role_arn": self.config.assume_role_arn},
                )

                role = sts.assume_role(
                    RoleArn=self.config.assume_role_arn,
                    RoleSessionName="GroveSecretHandler",
                )
                self._ssm = session.client(
                    "ssm",
                    region_name=self.config.ssm_region,
                    aws_access_key_id=role["Credentials"]["AccessKeyId"],
                    aws_secret_access_key=role["Credentials"]["SecretAccessKey"],
                    aws_session_token=role["Credentials"]["SessionToken"],
                )
        except (ClientError, BotoCoreError, KeyError) as err:
            raise AccessException(
                f"Secrets handler was unable to access AWS SSM: {err}"
            )

    def get(self, id: str) -> str:
        """Gets and returns an encrypted parameter from AWS SSM.

        :param id: The path of the secret to retrieve.

        :return: The decrypted and plain-text secret.
        """
        try:
            parameter = self._ssm.get_parameter(Name=id, WithDecryption=True)
        except (ClientError, BotoCoreError) as err:
            raise AccessException(
                f"Secrets handler failed to read secret from AWS SSM path {id}: {err}"
            )

        return jmespath.search("Parameter.Value", parameter)

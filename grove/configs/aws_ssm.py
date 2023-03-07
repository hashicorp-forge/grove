# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove AWS SSM parameter store configuration handler."""

import json
import logging
import os
from typing import List, Optional

from boto3.session import Session
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseSettings, Field, ValidationError

from grove.configs import BaseConfig
from grove.exceptions import AccessException, ConfigurationException
from grove.helpers import parsing
from grove.models import ConnectorConfig


class Configuration(BaseSettings):
    """Defines environment variables used to configure the AWS SSM handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    prefix: Optional[str] = Field(
        default="/grove/connectors/",
        description="A prefix to added to the beginning of all parameter store paths.",
    )
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
        variable `GROVE_CONFIG_AWS_SSM_ASSUME_ROLE_ARN`.
        """

        env_prefix = "GROVE_CONFIG_AWS_SSM_"
        case_insensitive = True


class Handler(BaseConfig):
    """A configuration handler to read configuration documents from AWS SSM."""

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
                    "Config handler is attempting to assume AWS role for SSM",
                    extra={"role_arn": self.config.assume_role_arn},
                )

                role = sts.assume_role(
                    RoleArn=self.config.assume_role_arn,
                    RoleSessionName="GroveConfigurationHandler",
                )
                self._ssm = session.client(
                    "ssm",
                    region_name=self.config.ssm_region,
                    aws_access_key_id=role["Credentials"]["AccessKeyId"],
                    aws_secret_access_key=role["Credentials"]["SecretAccessKey"],
                    aws_session_token=role["Credentials"]["SessionToken"],
                )
        except (ClientError, BotoCoreError, KeyError) as err:
            raise AccessException(f"Config handler failed to access AWS SSM: {err}")

    def get(self, id: str = "/") -> List[ConnectorConfig]:
        """Gets and returns one or more connector configuration objects from AWS SSM.

        Configuration documents are enumerated by their path, allowing multiple
        connectors to be defined in the SSM parameter store under a common path.

        :param id: The path of the parameter(s) to return. This is combined with the
            configured prefix, if set.

        :raises AccessException: An issue occurred when querying the value from SSM.

        :return: A list of connector configuration objects.
        """
        # Always combine the configured prefix with the specified path.
        path = f"{self.config.prefix.rstrip('/')}/{id.lstrip('/')}"  # type: ignore

        # Handling is performed in two-stages to reduce nested exception handlers and
        # to prevent a single bad connector configuration from causing all to fail.
        # Failure to load a single configuration should NEVER cause Grove to fail.
        pager = self._ssm.get_paginator("get_parameters_by_path")
        pages = pager.paginate(Path=path, Recursive=True, WithDecryption=True)
        candidates = {}

        try:
            for page in pages:
                for parameter in page.get("Parameters", {}):
                    name = parameter.get("Name")
                    value = parameter.get("Value")

                    if name and value:
                        candidates[name] = value
                        continue

                    self.logger.error(
                        "Config handler failed to fetch a connector configuration",
                        extra={"path": path, "key": name},
                    )
        except (ClientError, BotoCoreError) as err:
            raise AccessException(
                f"Config handler failed to read value from SSM path '{path}': {err}"
            )

        # Generate a list of documents for later processing.
        connectors = []

        for name, value in candidates.items():
            try:
                connectors.append(ConnectorConfig(**json.loads(value)))
            except (json.JSONDecodeError, ValidationError) as err:
                self.logger.error(
                    "Unable to load connector configuration",
                    extra={"document": name, "exception": err},
                )
                continue

        return connectors

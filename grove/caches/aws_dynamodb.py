# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove AWS DynamoDB cache handler."""

import logging
import os
from typing import Any, Dict, Optional

from boto3.session import Session
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseSettings, Field, ValidationError

from grove.caches import BaseCache
from grove.exceptions import (
    AccessException,
    ConfigurationException,
    DataFormatException,
    NotFoundException,
)
from grove.helpers import parsing


class Configuration(BaseSettings):
    """Defines environment variables used to configure the AWS DynamoDB handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    table: str = Field(
        default="grove",
        description="The name of the AWS DynamoDB table to use for the cache.",
    )
    url: Optional[str] = Field(
        description="An optional URL to use when connecting to AWS DynamoDB.",
        default=None,
    )
    assume_role_arn: Optional[str] = Field(
        description="An optional AWS role to assume when authenticating with AWS.",
        default=None,
    )
    table_region: Optional[str] = Field(
        description="The region that the DynamoDB table exists in (default us-east-1)",
        default=os.environ.get("AWS_REGION", "us-east-1"),
    )

    class Config:
        """Allow environment variable override of configuration fields.

        This also enforce a prefix for all environment variables for this handler. As
        an example the field `assume_role_arn` would be set using the environment
        variable `GROVE_CACHE_AWS_DYNAMODB_ASSUME_ROLE_ARN`.
        """

        env_prefix = "GROVE_CACHE_AWS_DYNAMODB_"
        case_insensitive = True


class Handler(BaseCache):
    """This cache handler allows Grove to write objects into an AWS DynamoDB cache."""

    def __init__(self):
        """Sets up access to DynamoDB

        This handler also attempt to assume a configured role in order to allow
        cross-account use - if required.

        :raises ConfigurationException: There was an issue with configuration.
        :raises AccessException: An issue occurred when accessing DynamoDB.
        """
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = Configuration()
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

        # Explicit calls to session are mostly used to allow mocks during testing.
        session = Session()
        session_arguments = {}

        if self.config.url:
            self.logger.debug(
                "Using DynamoDB URL from environment variables.",
                extra={"url": self.config.url},
            )
            session_arguments["endpoint_url"] = self.config.url

        try:
            if not self.config.assume_role_arn:
                self._store = session.client(
                    "dynamodb",
                    region_name=self.config.table_region,
                    **session_arguments,
                )
            else:
                self.logger.debug(
                    "Config handler is attempting to assume AWS role for SSM",
                    extra={"role_arn": self.config.assume_role_arn},
                )

                # We don't use session arguments for STS calls. Though perhaps there is
                # a situation where this is needed...?
                sts = session.client("sts")

                role = sts.assume_role(
                    RoleArn=self.config.assume_role_arn,
                    RoleSessionName="GroveDynamoDBHandler",
                )
                self._store = session.client(
                    "dynamodb",
                    region_name=self.config.table_region,
                    aws_access_key_id=role["Credentials"]["AccessKeyId"],
                    aws_secret_access_key=role["Credentials"]["SecretAccessKey"],
                    aws_session_token=role["Credentials"]["SessionToken"],
                )
        except (ClientError, BotoCoreError, KeyError) as err:
            raise AccessException(f"Cache handler failed to access AWS DynamoDB: {err}")

    def get(self, pk: str, sk: str) -> str:
        """Retrieve an value with the given PK / SK.

        :param pk: Partition key of the value to retrieve.
        :param sk: Sort key of the value to retrieve.

        :raises NotFoundException: No value was found.
        :raises AccessException: An issue occurred when getting the value.

        :return: Value from the cache.
        """
        try:
            response = self._store.get_item(
                TableName=self.config.table,
                Key={"pk": {"S": pk}, "sk": {"S": sk}},
            )
            pointer = response["Item"]["data"]["S"]
        except ClientError as err:
            self.logger.error(
                f"Unable to get value from cache. {err}", extra={"pk": pk, "sk": sk}
            )
            raise AccessException(err)
        except KeyError:
            self.logger.info("No value found in cache", extra={"pk": pk, "sk": sk})
            raise NotFoundException()

        return str(pointer)

    def set(
        self,
        pk: str,
        sk: str,
        value: str,
        not_set: bool = False,
        constraint: Optional[str] = None,
    ):
        """Stores the value for the given key in DynamoDB.

        :param pk: Partition key of the value to store.
        :param sk: Sort key of the value to store.
        :param value: Value to store.
        :param not_set: Specifies whether the value must not already be set in the
            cache for the set to be successful.
        :param constraint: An optional condition to use set operation. If provided,
            the currently cached value must match for the delete to be successful.

        :raises ValueError: An incompatible set of parameters were provided.
        :raises AccessException: An issue occurred when storing the value.
        :raises DataFormatException: The provided constraint was not satisfied.
        """
        options: Dict[str, Any] = {}
        options["ExpressionAttributeValues"] = {":data": {"S": str(value)}}

        if not_set and constraint is not None:
            raise ValueError("A value cannot both have a constraint AND not be set.")

        # Construct an appropriate filter based on input parameters.
        if not_set:
            options["ConditionExpression"] = "attribute_not_exists(#data)"

        if constraint is not None:
            options["ConditionExpression"] = "#data = :constraint"
            options["ExpressionAttributeValues"][":constraint"] = {"S": str(constraint)}

        # Attempt to set the item.
        try:
            self._store.update_item(
                TableName=self.config.table,
                Key={"pk": {"S": str(pk)}, "sk": {"S": str(sk)}},
                UpdateExpression="SET #data = :data",
                ExpressionAttributeNames={"#data": "data"},
                **options,
            )
        except ClientError as err:
            # Handle conditional check failures differently as these may indicate
            # concurrent execution.
            error_type = err.response.get("Error", {}).get("Code", "")

            if error_type == "ConditionalCheckFailedException":
                self.logger.error(
                    "Cache set failed as constraint failed.",
                    extra={
                        "pk": pk,
                        "sk": sk,
                        "value": value,
                        "not_set": not_set,
                        "constraint": constraint,
                    },
                )
                raise DataFormatException(err)

            # For everything else, just raise a generic error.
            self.logger.error(
                f"Unable to set value in cache: {err}",
                extra={
                    "pk": pk,
                    "sk": sk,
                    "value": value,
                    "not_set": not_set,
                    "constraint": constraint,
                },
            )
            raise AccessException(err)

    def delete(self, pk: str, sk: str, constraint: Optional[str] = None):
        """Deletes an entry from DynamoDB that has the given PK / SK.

        :param pk: Partition key of the value to delete.
        :param sk: Sort key of the value to delete.
        :param constraint: An optional condition to use during the delete. The value
            provided as the condition must match for the delete to be successful.

        :raises AccessException: An issue occurred when deleting the value.
        :raises DataFormatException: The provided constraint was not satisfied.
        """
        options: Dict[str, Any] = {}

        # Apply a constraint, if set.
        if constraint is not None:
            options["ConditionExpression"] = "#data = :constraint"
            options["ExpressionAttributeNames"] = {"#data": "data"}
            options["ExpressionAttributeValues"] = {
                ":constraint": {"S": str(constraint)}
            }

        try:
            self._store.delete_item(
                TableName=self.config.table,
                Key={"pk": {"S": pk}, "sk": {"S": sk}},
                **options,
            )
        except ClientError as err:
            # Handle conditional check failures differently as these may indicate
            # concurrent execution.
            error_type = err.response.get("Error", {}).get("Code", "")

            if error_type == "ConditionalCheckFailedException":
                self.logger.error(
                    "Cache set failed as constraint failed.",
                    extra={"pk": pk, "sk": sk, "constraint": constraint},
                )
                raise DataFormatException(err)

            # For everything else, just raise a generic error.
            self.logger.error(
                f"Unable to delete value from cache: {err}",
                extra={"pk": pk, "sk": sk, "constraint": constraint},
            )
            raise AccessException(err)

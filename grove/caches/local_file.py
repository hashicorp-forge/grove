# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove local file cache handler."""

import logging
import os
from typing import Optional

from pydantic import BaseSettings, Field, ValidationError

from grove.caches import BaseCache
from grove.exceptions import (
    AccessException,
    ConfigurationException,
    DataFormatException,
    NotFoundException,
)
from grove.helpers import parsing

CACHE_PATH = "{pk}/{sk}.cache"


class Configuration(BaseSettings):
    """Defines environment variables used to configure the local file cache handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    path: str = Field(
        description="The path to the directory to write cache data to.",
    )

    class Config:
        """Allow environment variable override of configuration fields.

        This also enforce a prefix for all environment variables for this handler.
        As an example the field `path` would be set using the environment variable
        `GROVE_CACHE_LOCAL_FILE_PATH`.
        """

        env_prefix = "GROVE_CACHE_LOCAL_FILE_"
        case_insensitive = True


class Handler(BaseCache):
    """A local file backed cache for pointers and other Grove data."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = Configuration()  # type: ignore
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

    def get(self, pk: str, sk: str) -> str:
        """Retrieve a value from a local file backed cache with the given key.

        :param pk: Partition key of the value to retrieve.
        :param sk: Sort key of the value to retrieve.

        :raises NotFoundException: No value was found.

        :return: Value from the cache.
        """
        path = os.path.join(self.config.path, CACHE_PATH.format(pk=pk, sk=sk))
        value = None

        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)

            with open(path, "r") as hndl:
                value = hndl.read()
        except FileNotFoundError:
            # If the file isn't found, we treat this as the cache is empty for this
            # PK / SK, so we can just drop through and let the is None handler take
            # care of this for us.
            pass
        except OSError as err:
            raise AccessException(f"Unable to read cache entry from {path}. {err}")

        if value is None:
            self.logger.info("No value found in cache", extra={"pk": pk, "sk": sk})
            raise NotFoundException("No value found in cache")

        return value

    def set(
        self,
        pk: str,
        sk: str,
        value: str,
        not_set: bool = False,
        constraint: Optional[str] = None,
    ):
        """Stores the value for the given key in a local file backed cache.

        :param pk: Partition key of the value to save.
        :param sk: Sort key of the value to save.
        :param value: Value to save.
        :param not_set: Specifies whether the value must not already be set in the cache
            for the set to be successful.
        :param constraint: An optional condition to use set operation. If provided,
            the currently cached value must match for the delete to be successful.

        :raises ValueError: An incompatible set of parameters were provided.
        :raises DataFormatException: The provided constraint was not satisfied.
        """
        current = None
        path = os.path.join(self.config.path, CACHE_PATH.format(pk=pk, sk=sk))

        if constraint is not None and not_set:
            raise ValueError("A value cannot both have a constraint AND not be set.")

        # First check if the value is set, and if so whether the caller requires that it
        # NOT already be set.
        try:
            current = self.get(pk, sk)
        except NotFoundException:
            pass

        if current and not_set:
            raise DataFormatException("Value is already set in cache")

        # Next check if the constraint is met.
        if constraint is not None and current != constraint:
            raise DataFormatException("Current value in cache did not match constraint")

        # Finally, set the value.
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)

            with open(path, "w") as hndl:
                hndl.truncate()
                hndl.write(value)
        except OSError as err:
            raise AccessException(f"Unable to write cache entry to {path}. {err}")

    def delete(self, pk: str, sk: str, constraint: Optional[str] = None):
        """Deletes an entry from local file backed cache that has the given key.

        :param pk: Partition key of the value to delete.
        :param sk: Sort key of the value to delete.
        :param constraint: An optional condition to use during the delete. The value
            provided as the condition must match for the delete to be successful.

        :raises DataFormatException: The provided constraint was not satisfied.
        """
        # To enforce constraints, we first need the current value - if any.
        current = None
        path = os.path.join(self.config.path, CACHE_PATH.format(pk=pk, sk=sk))

        try:
            current = self.get(pk, sk)
        except NotFoundException:
            pass

        # Next check if the constraint is met.
        if constraint is not None and current != constraint:
            raise DataFormatException("Current value in cache did not match constraint")

        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError as err:
            raise AccessException(f"Unable to delete cache entry from {path}. {err}")

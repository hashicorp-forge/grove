# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove in memory cache handler."""

import logging
from typing import Optional

from grove.caches import BaseCache
from grove.exceptions import DataFormatException, NotFoundException


class Handler(BaseCache):
    """A volatile in-memory backed cache for pointers and other Grove data."""

    def __init__(self):
        self._data = {}
        self.logger = logging.getLogger(__name__)

    def get(self, pk: str, sk: str) -> str:
        """Retrieve a value with the given PK / SK.

        :param pk: Partition key of the value to retrieve.
        :param sk: Sort key of the value to retrieve.

        :raises NotFoundException: No value was found.

        :return: Value from the cache.
        """
        value = self._data.get(pk, {}).get(sk, None)

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
        """Stores the value for the given key in a local dict.

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
        if constraint is not None and not_set:
            raise ValueError("A value cannot both have a constraint AND not be set.")

        # First check if the value is set, and if so whether the caller requires that it
        # NOT already be set.
        current = None

        try:
            current = self._data[pk][sk]
        except KeyError:
            pass

        if current and not_set:
            raise DataFormatException("Value is already set in cache")

        # Next check if the constraint is met.
        if constraint is not None and current != constraint:
            raise DataFormatException("Current value in cache did not match constraint")

        # Finally, set the value.
        if pk not in self._data:
            self._data[pk] = {}

        self._data[pk][sk] = value

    def delete(self, pk: str, sk: str, constraint: Optional[str] = None):
        """Deletes an entry from dict that has the given PK / SK.

        :param pk: Partition key of the value to delete.
        :param sk: Sort key of the value to delete.
        :param constraint: An optional condition to use during the delete. The value
            provided as the condition must match for the delete to be successful.

        :raises DataFormatException: The provided constraint was not satisfied.
        """
        # To enforce constraints, we first need the current value - if any.
        current = None

        try:
            current = self._data[pk][sk]
        except KeyError:
            pass

        # Next check if the constraint is met.
        if constraint is not None and current != constraint:
            raise DataFormatException("Current value in cache did not match constraint")

        if pk in self._data:
            if sk in self._data[pk]:
                del self._data[pk][sk]

            if not self._data[pk]:
                del self._data[pk]

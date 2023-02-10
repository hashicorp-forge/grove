# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove cache handlers."""

import abc
from typing import Optional


class BaseCache(abc.ABC):
    @abc.abstractmethod
    def get(self, pk: str, sk: str) -> str:
        """Gets the value for the given key from the cache.

        If the implementation does not differentiate partition and sort keys, these
        values should be combined in an appropriate way to form a cache key.

        :param pk: Partition key of the value to retrieve.
        :param sk: Sort key of the value to retrieve.

        :return: The value from the cache.
        """
        pass

    @abc.abstractmethod
    def set(
        self,
        pk: str,
        sk: str,
        value: str,
        not_set: bool = False,
        constraint: Optional[str] = None,
    ):
        """Stores the value for the given key in a cache.

        If the implementation does not differentiate partition and sort keys, these
        values should be combined in an appropriate way to form a cache key.

        :param pk: Partition key of the value to store.
        :param sk: Sort key of the value to store.
        :param value: Value to store.
        :param not_set: Specifies whether the value must not already be set in the cache
            for the set to be successful.
        :param constraint: An optional condition to use set operation. If provided,
            the currently cached value must match for the delete to be successful.

        :raises ValueError: An incompatible set of parameters were provided.
        :raises DataFormatException: The provided constraint was not satisfied.
        """
        pass

    @abc.abstractmethod
    def delete(self, pk: str, sk: str, constraint: Optional[str] = None):
        """Deletes an entry with the given key from the cache.

        If the implementation does not differentiate partition and sort keys, these
        values should be combined in an appropriate way to form a cache key.

        :param pk: Partition key of the value to delete.
        :param sk: Sort key of the value to delete.
        :param constraint: An optional condition to use during the delete. The value
            provided as the condition must match for the delete to be successful.

        :raises DataFormatException: The provided constraint was not satisfied.
        """
        pass

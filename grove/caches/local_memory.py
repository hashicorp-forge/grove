"""Grove in memory cache handler."""

import logging

from grove.caches import BaseCache
from grove.exceptions import NotFoundException


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

    def set(self, pk: str, sk: str, value: str, condition: str) -> None:
        """Stores the value for the given key in a local dict.

        :param pk: Partition key of the value to save.
        :param sk: Sort key of the value to save.
        :param value: Value to save.
        :param condition: Unused in this implementation.
        """
        self._data.setdefault(pk, {})[sk] = value

    def delete(self, pk: str, sk: str) -> None:
        """Deletes an entry from dict that has the given PK / SK.

        :param pk: Partition key of the value to delete.
        :param sk: Sort key of the value to delete.
        """
        if pk in self._data:
            if sk in self._data[pk]:
                del self._data[pk][sk]

            if not self._data[pk]:
                del self._data[pk]

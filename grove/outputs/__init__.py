# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides collected Grove log output to supported destinations."""

import abc
import gzip
import json
from typing import Any, Dict, List

from grove.constants import GROVE_METADATA_KEY
from grove.exceptions import DataFormatException


class BaseOutput(abc.ABC):
    @abc.abstractmethod
    def submit(
        self,
        data: bytes,
        connector: str,
        identity: str,
        operation: str,
    ):
        """Implements logic require to write collected log data to the given backend.

        :param data: Log data to write.
        :param connector: Name of the connector which retrieved the data.
        :param identity: Identity the collected data was collect for.
        :param operation: Operation the collected logs are associated with.
        """
        pass

    def serialize(self, data: List[Any], metadata: Dict[str, Any]) -> bytes:
        """Serialize data to a standard format (gzipped NDJSON).

        :param data: A list of log entries to serialize to JSON.
        :param metadata: Metadata to append to JSON before serialisation.

        :return: Log data serialized as gzipped NDJSON (as bytes).

        :raises DataFormatException: Cannot serialize the input to JSON.
        """
        candidate = []

        # Append the Grove metadata to each log entry, and serialize to JSON. Adding
        # This is expensive but we can't just json.dumps into gzip.compress as that
        # will not yield NDJSON.
        for entry in data:
            entry[GROVE_METADATA_KEY] = metadata

            # We don't want to silently drop and lose single records, so drop the entire
            # batch if there is bad data (which will trigger a retry next run).
            try:
                candidate.append(json.dumps(entry, separators=(",", ":")))
            except TypeError as err:
                message = f"Unable to serialize to JSON: {err}"
                raise DataFormatException(message)

        return gzip.compress(bytes("\r\n".join(candidate), "utf-8"))

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides collected Grove log output to supported destinations."""

import abc
import gzip
import json
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseSettings, Extra, ValidationError

from grove.constants import GROVE_METADATA_KEY
from grove.exceptions import ConfigurationException, DataFormatException
from grove.helpers import parsing


class BaseOutput(abc.ABC):
    """The basis for all Grove output handlers."""

    class Configuration(BaseSettings, extra=Extra.allow):
        """Defines the configuration directives required by all output handlers."""

        pass

    def __init__(self):
        """Implements core logic which applies to all handlers.

        This includes configuration of logging, and parsing of configuration.
        """
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = self.Configuration()
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

    def setup(self):
        """Implements logic to setup any required clients, sockets, or connections.

        If not required for the given output handler, this may be a no-op.
        """
        pass

    @abc.abstractmethod
    def submit(
        self,
        data: bytes,
        connector: str,
        identity: str,
        operation: str,
        part: int = 0,
        suffix: Optional[str] = None,
        descriptor: Optional[str] = None,
    ):
        """Implements logic require to write collected log data to the given backend.

        :param data: Log data to write.
        :param connector: Name of the connector which retrieved the data.
        :param identity: Identity the collected data was collect for.
        :param operation: Operation the collected logs are associated with.
        :param part: Number indicating which part of the same log stream this file
            contains data for. This is used to indicate that the logs are from the same
            collection, but have been broken into smaller files for downstream
            processing.
        :param suffix: An optional suffix to allow propagation of file type information
            or other relevant features.
        :param descriptor: An optional and arbitrary descriptor associated with the
            log data. This may be used by handlers for construction / specification of
            file paths, URLs, or database tables.
        """
        pass

    def serialize(self, data: List[Any], metadata: Dict[str, Any] = {}) -> bytes:
        """Implements serialization of log entries to a gzipped NDJSON.

        :param data: A list of log entries to serialize to JSON.
        :param metadata: Metadata to append to each log entry before serialization. If
            not specified no metadata will be added.

        :return: Log data serialized as gzipped NDJSON (as bytes).

        :raises DataFormatException: Cannot serialize the input to JSON.
        """
        candidate = []

        # Append the Grove metadata to each log entry, and serialize to JSON. Adding
        # This is expensive but we can't just json.dumps into gzip.compress as that
        # will not yield NDJSON.
        for entry in data:
            # Skip empty log entries.
            if entry is None:
                continue

            if metadata:
                entry[GROVE_METADATA_KEY] = {
                    **metadata,
                    **entry.get(GROVE_METADATA_KEY, {}),
                }

            # We don't want to silently drop and lose single records, so drop the entire
            # batch if there is bad data (which will trigger a retry next run).
            try:
                candidate.append(json.dumps(entry, separators=(",", ":")))
            except TypeError as err:
                raise DataFormatException(f"Unable to serialize to JSON: {err}")

        return gzip.compress(bytes("\r\n".join(candidate), "utf-8"))

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove stdout output handler."""

import datetime
import json
from typing import Any, Dict, List, Optional

from grove.constants import DATESTAMP_FORMAT, GROVE_METADATA_KEY
from grove.exceptions import DataFormatException
from grove.outputs import BaseOutput


class Handler(BaseOutput):
    def submit(
        self,
        data: bytes,
        connector: str,
        identity: str,
        operation: str,
        part: int = 0,
        kind: Optional[str] = "json",
        descriptor: Optional[str] = "raw",
    ):
        """Print captured data to stdout.

        :param data: Log data to write.
        :param connector: Name of the connector which retrieved the data.
        :param identity: Identity the collected data was collect for.
        :param operation: Operation the collected logs are associated with.
        :param part: Number indicating which part of the same log stream this file
            contains data for. This is used to indicate that the logs are from the same
            collection, but have been broken into smaller files for downstream
            processing.
        :param kind: The format of the data being output.
        :param descriptor: An arbitrary descriptor to identify the data being output.

        """
        datestamp = datetime.datetime.utcnow()

        for entry in data.decode("utf-8").splitlines():
            print(
                json.dumps(
                    {
                        "part": part,
                        "kind": kind,
                        "descriptor": descriptor,
                        "connector": connector,
                        "identity": identity,
                        "operation": operation,
                        "datestamp": datestamp.strftime(DATESTAMP_FORMAT),
                        "message": json.loads(entry),
                    }
                ),
                flush=True,
            )

    def serialize(self, data: List[Any], metadata: Dict[str, Any] = {}) -> bytes:
        """Serialize data to a standard format (NDJSON).

        :param data: A list of log entries to serialize to JSON.
        :param metadata: Metadata to append to each log entry before serialization. If
            not specified no metadata will be added.

        :return: Log data serialized as NDJSON.

        :raises DataFormatException: Cannot serialize the input to JSON.
        """
        candidate = []

        for entry in data:
            entry[GROVE_METADATA_KEY] = metadata

            # We don't want to silently drop and lose single records, so drop the entire
            # batch if there is bad data (which will trigger a retry next run).
            try:
                candidate.append(json.dumps(entry, separators=(",", ":")))
            except TypeError as err:
                message = f"Unable to serialize to JSON: {err}"
                raise DataFormatException(message)

        return bytes("\r\n".join(candidate), "utf-8")

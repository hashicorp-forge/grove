# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove processor to zip two sets of data into a dictionary of key / value pairs.

This processor is useful for transforming lists of key / value pairs, where the key and
the value of a desired set of data. This processor is useful for flattening key / value
data from sources such as Google Workspace activity logs.

In line with this Google Workspace example, data from Google may appear as follows:

    "parameters": [
        {"name": "owner", "value": "a-user@example.org"},
        {"name": "visibility", "value": "private"}
    ]

Unfortunately, this data can be hard to work with in many SIEMs and search indexes as it
is expressed in the raw log entries. As a result, this processor may be used to instead
'flatten' this key / value data into a dictionary which is keyed by the extracted value
of "name", and uses the value from the "value" field:

    "parameters": {
        "owner": "a-user@example.org",
        "visibility": "private"
    }

Making this data considerably easier to work with during creation of indexes, and
creation of detection content.
"""

from typing import Any, Dict, List

import jmespath
from pydantic import Extra

from grove.helpers import parsing
from grove.models import ProcessorConfig
from grove.processors import BaseProcessor


class Handler(BaseProcessor):
    """Extract and map fields using JMESPaths."""

    class Configuration(ProcessorConfig, extra=Extra.forbid):
        """Expresses the configuration and associated validators for the processor."""

        # Source defines the JMESPath to the data which needs to be zipped.
        source: str

        # Key defines the JMESPath of the data to use as keys in the constructed
        # dictionary. This must be the path relative to the source, not the absolute
        # path.
        key: str

        # Values defines the JMESPaths of the data to use as values in the constructed
        # dictionary. If multiple are provided, the sources are processed in order with
        # the first match winning. This must be the path relative to the source, not the
        # absolute path.
        values: List[str] = []

    def process(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract and zip configured paths, replacing the source.

        :param entry: A collected log entry.

        :return: The processed log entry with fields zipped.
        """
        result: Dict[str, Any] = {}
        children: List[Any] = []

        # If the source field cannot be found, just pass the record back to the caller
        # as we don't want to drop it. We also want to make sure we can always iterate
        # over the children, so if the value isn't a list, map it into one.
        candidate = jmespath.search(self.configuration.source, entry)
        if candidate is None:
            return [entry]

        if isinstance(candidate, list):
            children = candidate
        else:
            children = [candidate]

        for child in children:
            # No key? Skip.
            key = jmespath.search(self.configuration.key, child)
            if key is None:
                continue

            # No values found? Skip.
            value = None
            for path in self.configuration.values:
                value = jmespath.search(path, child)
                if value is not None:
                    break

            if value is None:
                continue

            # If we have both save it and move on.
            result[key] = value

        # Map the processed data over the top of the original.
        processed = parsing.update_path(
            entry,
            parsing.quote_aware_split(self.configuration.source),
            result,
            replace=True,
        )

        # Return the newly processed entry. A list is always used, even if only a single
        # element is returned, to allow support for dropping log entries, or splitting a
        # single log entry into multiple.
        return [processed]

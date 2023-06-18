# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove processor to extract and map fields using JMESPaths.

This processor is intended to be used to transform raw log entries into a common schema.
This is especially useful for ensuring that all collected log entries from differing
upstream vendors are in a consistent format - whether industry standard, or bespoke.
"""

import json
from typing import Any, Dict, List, Optional

import jmespath
from pydantic import BaseModel, Extra, validator

from grove.helpers import parsing
from grove.models import ProcessorConfig
from grove.processors import BaseProcessor


class Mapping(BaseModel, extra=Extra.forbid):
    """Expresses the configuration fields used to specify path mapping."""

    # Destination specifies where to write extracted or specified values into. This
    # can be a nested path, with subsequent dimensions specified with dots (`.`).
    destination: str

    # Sources defines a list of JMESPaths to map into the destination. If multiple
    # are provided, the sources are processed in order with the first match winning.
    sources: List[str] = []

    # Static allows a static field to be written into the destination, rather than
    # extraction from the source. This field is incompatible with sources.
    static: Optional[str]

    @validator("static")
    def static_or_sources(cls, value, values):
        """Ensures that either sources or static is set, not both."""
        if value and len(values.get("sources")) > 0:
            raise ValueError("Either sources or static should be set, not both.")

        return value


class Handler(BaseProcessor):
    """Extract and map fields using JMESPaths."""

    class Configuration(ProcessorConfig, extra=Extra.forbid):
        """Expresses the configuration and associated validators for the processor."""

        # Remap the original event as a string under the provided path. If not set, any
        # field not mapped will be dropped.
        raw: Optional[str]

        # Defines the field mapping.
        fields: List[Mapping]

    def process(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Attempt to extract and map fields from the log entry.

        :param entry: A collected log entry.

        :return: The processed log entry with fields mapped, as a list.
        """
        result: Dict[str, Any] = {}

        # Map the entire log entry under the given path - if configured.
        if self.configuration.raw:
            result = parsing.update_path(
                result,
                parsing.quote_aware_split(self.configuration.raw),
                json.dumps(entry, separators=(",", ":")),
            )

        for field in self.configuration.fields:
            value = field.static
            destination = parsing.quote_aware_split(field.destination)

            # If a static value is defined it should be used over any source fields.
            if not value:
                # Mappings may contain multiple sources to attempt to map. These are
                # evaluated from the first entry to the last, with the first match
                # winning.
                for source in field.sources:
                    value = jmespath.search(source, entry)
                    if value:
                        break

            # Combine the extracted value with the data nested under the same path - or
            # create the path if not present.
            result = parsing.update_path(result, destination, value)

        # Return the newly processed entry. A list is always used, even if only a single
        # element is returned, to allow support for dropping log entries, or splitting a
        # single log entry into multiple.
        return [result]

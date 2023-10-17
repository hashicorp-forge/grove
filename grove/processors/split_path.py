# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove processor to split a log entry into N log entries by the specified JMESPath.

This processor is intended to allow "fanning-out" a single log entry which contains
several related operations into distinct log entries per item. The remainder of the
log entry outside of the split path will not be modified.

It is important to note that the list of elements which is fanned-out will be converted
into a dictionary, rather than a list.

As an example, in following sample:

    {
        "events": [
            {"name": "First", "value": 1},
            {"name": "Second", "value": 2},
        ]
    }

After splitting based on "events", two records would be generated, which contain the
following:

    {
        "events": {"name": "First", "value": 1},
    }

    {
        "events": {"name": "Second", "value": 2},
    }

"""

from typing import Any, Dict, List

import jmespath
from pydantic import Extra

from grove.helpers import parsing
from grove.models import ProcessorConfig
from grove.processors import BaseProcessor


class Handler(BaseProcessor):
    """Split a log entry into N log entries by the specified JMESPath."""

    class Configuration(ProcessorConfig, extra=Extra.forbid):
        """Expresses the configuration and associated validators for the processor."""

        # Source defines the path to split the log entry by. This should be defined as a
        # JMESPath. The field referenced by this path should be a list.
        source: str

    def process(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Attempt to extract and map fields from the log entry.

        :param entry: A collected log entry.

        :return: The processed log entry.
        """
        # In this instance we WANT to mutate the copy outside of the processor.
        processed = []
        children = jmespath.search(self.configuration.source, entry)

        # To ensure we don't accidentally try and split a string or dictionary we need
        # to make sure that the type of the found children - if any - is correct.
        if not children or not isinstance(children, list) or len(children) < 1:
            return [entry]

        for child in children:
            processed.append(
                parsing.update_path(
                    parsing.quick_copy(entry),
                    parsing.quote_aware_split(self.configuration.source),
                    child,
                    replace=True,
                )
            )

        return processed

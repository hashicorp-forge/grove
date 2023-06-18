# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove processor to split a log entry into N log entries by the specified JMESPath.

This processor is intended to allow "fanning-out" a single log entry which contains
several related operations into distinct log entries per item. The remainder of the
log entry outside of the split path will not be modified.
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

        if len(children) <= 1:
            return [entry]

        for child in children:
            processed.append(
                parsing.update_path(
                    parsing.quick_copy(entry),
                    parsing.quote_aware_split(self.configuration.source),
                    [child],
                    replace=True,
                )
            )

        return processed

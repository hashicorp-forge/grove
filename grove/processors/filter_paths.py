# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove processor to filter (delete) fields from log entries based on provided paths.

This processor is intended to allow removal of superfluous or duplicated data from
log entries. This may be used after a processing stage to remove the original source
data, or used to prune down a log entry from a particularly verbose vendor.
"""

from typing import Any, Dict, List

from pydantic import Extra

from grove.helpers import parsing
from grove.models import ProcessorConfig
from grove.processors import BaseProcessor


class Handler(BaseProcessor):
    """Filter (delete) fields from log entries based on provided paths."""

    class Configuration(ProcessorConfig, extra=Extra.forbid):
        """Expresses the configuration and associated validators for the processor."""

        # Source defines a list of paths to field to drop (delete). These should be
        # defined as a JMESPaths.
        sources: List[str]

    def process(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Attempt to drop a configured field from the log entry.

        :param entry: A collected log entry.

        :return: The processed log entry, with fields dropped.
        """
        for source in self.configuration.sources:
            entry = parsing.update_path(
                entry,
                parsing.quote_aware_split(source),
                None,
            )

        return [entry]

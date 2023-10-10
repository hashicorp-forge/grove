# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove processor to filter (delete) entire log entries based JMESPath queries.

This processor is intended to allow dropping of log entries when a set of criteria is
met. This may be used to assist in reducing outputting noisy log entries where a given
vendor does not provide a mechanism for filtering events.
"""

import jmespath
from typing import Any, Dict, List

from pydantic import Extra

from grove.models import ProcessorConfig
from grove.processors import BaseProcessor


class Handler(BaseProcessor):
    """Filter (delete) log entries based on JMESPath queries.

    If any of the configured filters match a given log entry (return True), then the log
    entry will be dropped. Queries are evaluated against log entries in the order that
    they are defined, and the 'first match wins'.
    """

    class Configuration(ProcessorConfig, extra=Extra.forbid):
        """Expresses the configuration and associated validators for the processor."""

        # Filters defines a list of JMESPath queries to be evaluated against each log
        # entry in order.
        filters: List[str]

    def process(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Drop log entries which do not match all of the configured JMESPath queries.

        :param entry: A collected log entry.

        :return: The processed log entry, or an empty array if the log entry should be
            dropped.
        """
        for filter in self.configuration.filters:
            if jmespath.search(filter, entry):
                return []

        return [entry]

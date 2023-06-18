# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Test connector for Grove."""

import time
from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException

DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Connector(BaseConnector):
    NAME = "local_heartbeat"
    POINTER_PATH = "timestamp"
    LOG_ORDER = CHRONOLOGICAL

    @property
    def count(self):
        """Fetches the count of heartbeat messages to emit from the configuration.

        :return: The number of heartbeat messages to emit.
        """
        try:
            return int(self.configuration.count)
        except (AttributeError, ValueError):
            return 5

    @property
    def interval(self):
        """Fetches the interval to emit a heartbeat message from the configuration.

        :return: The heartbeat interval, in seconds.
        """
        try:
            return int(self.configuration.interval)
        except (AttributeError, ValueError):
            return 1

    def collect(self):
        """Generates test log entries at the configured interval."""
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.utcnow() - timedelta(days=7)).isoformat()

        for _ in range(self.count):
            # Writes this batch of entries to the output and updates the pointer.
            self.save(
                [
                    {
                        "type": "heartbeat",
                        "timestamp": datetime.utcnow().strftime(DATESTAMP_FORMAT),
                    }
                ]
            )
            time.sleep(self.interval)

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Torq audit log connector for Grove."""

from datetime import datetime, timedelta, timezone

from grove.connectors import BaseConnector
from grove.connectors.torq.api import Client
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "torq_audit_logs"
    POINTER_PATH = "timestamp"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    def collect(self):
        """Collects all audit logs from the Torq API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, a 7 day look-back of data will be collected.
        """
        client = Client(identity=self.identity, key=self.key)
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to 7-days ago.
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

        while True:
            log = client.get_audit_logs(start_time=self.pointer, cursor=cursor)

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

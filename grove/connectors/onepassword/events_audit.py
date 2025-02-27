# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""1Password Audit event log connector for Grove."""

from grove.connectors import BaseConnector
from grove.connectors.onepassword.api import Client
from grove.connectors.onepassword.util import get_pointer_values
from grove.constants import CHRONOLOGICAL


class Connector(BaseConnector):
    CONNECTOR = "onepassword_events_audit"
    POINTER_PATH = "cursor"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        """Collects all logs from the 1Password audit event log API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(token=self.key)

        # self.pointer could start out as either a cursor or ISO timestamp depending on if this
        # is an upgrade. That said, once we reach here, cursor will always be used as the
        # pointer. self.pointer is also used elsewhere for content like saving to the cache.
        cursor, start_time = get_pointer_values(self)
        self.pointer = cursor

        # Get log data from the upstream API, paging as required.
        while True:
            log, has_more = client.get_auditevents(
                start_time=start_time,
                cursor=cursor,
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if not has_more:
                break

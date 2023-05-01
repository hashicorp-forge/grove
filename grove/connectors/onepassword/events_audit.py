# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""1Password Audit event log connector for Grove."""

import datetime

from grove.connectors import BaseConnector
from grove.connectors.onepassword.api import Client
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "onepassword_events_audit"
    POINTER_PATH = "timestamp"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        """Collects all logs from the 1Password audit event log API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(token=self.key)
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago. In the case of the 1Password API this is an ISO
        # timestamp in a field called "timestamp".
        try:
            _ = self.pointer
        except NotFoundException:
            week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
            self.pointer = (week_ago).astimezone().replace(microsecond=0).isoformat()

        # Get log data from the upstream API, paging as required.
        while True:
            log = client.get_auditevents(start_time=self.pointer, cursor=cursor)

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

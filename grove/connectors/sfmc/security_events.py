# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Marketing Cloud Security security event connector for Grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.sfmc.api import Client
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "sfmc_security_events"
    POINTER_PATH = "createdDate"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        """Collects all logs from the SFMC API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(identity=self.identity, token=self.key)
        cursor = 1

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago.
        now = datetime.utcnow()
        isoformat = {"sep": "T", "timespec": "milliseconds"}
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (now - timedelta(days=7)).isoformat(**isoformat)

        # Get log data from the upstream API. A "enddate" argument is used here as the
        # API specification simply says that "the default is today". It is unclear
        # whether this is midnight today, or "now".
        while True:
            log = client.get_audit_events(
                cursor=cursor,
                startdate=self.pointer,
                enddate=now.isoformat(**isoformat),
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor  # type: ignore
            if not cursor:
                break

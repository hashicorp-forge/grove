# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Dropbox team event log connector for Grove."""

import datetime

from grove.connectors import BaseConnector
from grove.connectors.dropbox.api import Client
from grove.constants import CHRONOLOGICAL, OPERATION_DEFAULT
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    CONNECTOR = "dropbox_team_events"
    POINTER_PATH = "timestamp"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        """Collects all logs from the Dropbox team event API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(token=self.key)
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago.
        try:
            _ = self.pointer
        except NotFoundException:
            week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
            self.pointer = (
                (week_ago).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
            )

        # There is no "All" for events in the Dropbox API. As a result, if all events
        # are required, the field needs to be omitted.
        category = None

        if self.operation != OPERATION_DEFAULT:
            category = self.operation

        # Get log data from the upstream API, paging as required.
        while True:
            log = client.get_events(
                start_time=self.pointer,
                category=category,
                cursor=cursor,
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

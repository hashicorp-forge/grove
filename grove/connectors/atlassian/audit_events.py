# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Atlassian Audit connector for Grove."""

from datetime import datetime, timedelta, timezone

from grove.connectors import BaseConnector
from grove.connectors.atlassian.api import Client
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException

ISO_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
ISO_TIMESTAMP_FORMAT_NO_MS = "%Y-%m-%dT%H:%M:%S%z"


class Connector(BaseConnector):
    NAME = "atlassian_audit_events"
    POINTER_PATH = "attributes.time"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    def collect(self):
        """Collects all logs from the Atlassian Events API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(identity=self.identity, token=self.key)
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago. In the case of the Atlassian events API the pointer is
        # the value of the "time" field from the latest record retrieved from the
        # API - which is an ISO8601 timestamp.
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.now(timezone.utc) - timedelta(days=7)).strftime(
                ISO_TIMESTAMP_FORMAT
            )

        # Atlassians org API from_date is The earliest date and time of the event
        # represented as a UNIX epoch time. To support this, the pointers need to be
        # CONVERTED to UNIX epoch time up to the millisecond from the value of the
        # pointer.
        #
        # The try / except block is required as two different time formats have been
        # seen returned from Atlassian for these event types: one with milliseconds, one
        # without.
        try:
            start = int(
                datetime.strptime(self.pointer, ISO_TIMESTAMP_FORMAT).timestamp() * 1000
            )
        except ValueError:
            start = int(
                datetime.strptime(self.pointer, ISO_TIMESTAMP_FORMAT_NO_MS).timestamp()
                * 1000
            )

        # Page over data using the cursor, saving returned data page by page.
        while True:
            log = client.get_audit(from_date=str(start), cursor=cursor)

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

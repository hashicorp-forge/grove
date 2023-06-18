# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zoom activity log connector for Grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.zoom import api
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException

DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Connector(BaseConnector):
    NAME = "zoom_activities"
    POINTER_PATH = "time"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    @property
    def client_id(self):
        """Fetches the Zoom ClientID from the configuration.

        This is required as this is a third authentication element required by Zoom.
        """
        try:
            return self.configuration.client_id
        except AttributeError:
            return None

    def collect(self):
        """Collects all logs from the Zoom Activity API."""
        client = api.Client(
            identity=self.identity,
            client_id=self.client_id,
            key=self.key,
        )

        # Use the refresh token to generate a temporary access token.
        client.get_access_token()

        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a day ago. This API uses a YYYYMMDD date range. We can input using
        # RFC 3339 format because the API converts the to and from to YYYYMMDD.
        now = datetime.utcnow().strftime(DATESTAMP_FORMAT)
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.utcnow() - timedelta(days=1)).strftime(
                DATESTAMP_FORMAT
            )

        # Get log data from the upstream API. "From" and "to" datetime query parameters
        # are required. This API only gets data from a YYYYMMDD date range.
        while True:
            log = client.get_activities(
                from_date=self.pointer, to_date=now, cursor=cursor
            )

            # Zoom is troublesome as their API only allows filtering by date, not time.
            # As a result of this we need to do an additional step to ignore anything
            # after our pointer.
            candidates = self.deduplicate_by_pointer(log.entries)

            # Save this batch of log entries.
            self.save(log.entries)

            # If any log entries were filtered, the pointer was found, so we can skip
            # everything else.
            if len(candidates) != len(log.entries):
                break

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

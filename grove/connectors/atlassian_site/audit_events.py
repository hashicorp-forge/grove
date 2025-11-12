# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Atlassian Audit connector for Grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.atlassian_site.api import Client
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException

DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class Connector(BaseConnector):
    CONNECTOR = "atlassian_site_audit_events"
    POINTER_PATH = "created"
    LOG_ORDER = CHRONOLOGICAL

    @property
    def username(self):
        """Fetches the Atlassian site user from the configuration.

        This is required as this is an additional authentication element required.

        :return: The "username" portion of the connector's configuration.
        """
        try:
            return self.configuration.username
        except AttributeError:
            return None

    def collect(self):
        """Collects all logs from the Atlassian site Audit API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(
            identity=self.identity,
            username=self.username,
            token=self.key,
        )

        # Set cursor
        cursor = 0

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago.
        now = datetime.utcnow().strftime(DATESTAMP_FORMAT)
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.utcnow() - timedelta(days=7)).strftime(
                DATESTAMP_FORMAT
            )

        # Get log data from the upstream API. A "from" and "to" datetime query
        # parameters are required.
        while True:
            log = client.get_audit(
                from_date=self.pointer,
                to_date=now,
                cursor=cursor,
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor  # type: ignore
            if not cursor:
                break

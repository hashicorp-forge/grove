# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Quay connector for Grove."""

from datetime import datetime, timedelta, timezone

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException
from grove.connectors.quay.api import Client


class Connector(BaseConnector):
    """Quay connector for Grove."""

    CONNECTOR = "quay_organization_logs"
    POINTER_PATH = "datetime"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        """Collects logs from the Quay organization logging api API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """

        # initialize quay.io client
        client = Client(identity=self.identity, token=self.key)
        cursor = None

         # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago. In the case of the PagerDuty audit API the pointer is
        # the value of the "execution_time" field from the latest record retrieved from
        # the API.
        try:
            _ = self.pointer
        except NotFoundException:
                since = datetime.now(timezone.utc) - timedelta(days=7)
                # need RFC2822/5322 for pointer/timestamp
                # note: email.utils method, format_datetime, returns a +0000 and not -0000,
                # which is what the api returns as a timestamp -> manually formatting 
                self.pointer = since.strftime("%a, %d %b %Y %H:%M:%S -0000")

        # page over data using the cursor, saving returned data page by page.
        while True:
            log = client.get_organization_logs(
                after=self.pointer,
                cursor=cursor,
                )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break
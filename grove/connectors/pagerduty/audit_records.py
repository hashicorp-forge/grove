# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""PagerDuty Audit connector for Grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.pagerduty.api import Client
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "pagerduty_audit_records"
    POINTER_PATH = "execution_time"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    def collect(self):
        """Collects PagerDuty audit records from the PagerDuty API.

        https://developer.pagerduty.com/api-reference/

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(token=self.key)
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago. In the case of the PagerDuty audit API the pointer is
        # the value of the "execution_time" field from the latest record retrieved from
        # the API - which is in ISO8601 Format.
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.utcnow() - timedelta(days=7)).isoformat()

        # Get log data from the upstream API, paging as required.
        while True:
            log = client.get_records(since=self.pointer, cursor=cursor)

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

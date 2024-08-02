# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""FleetDM Vulnerability connector for Grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.slack.api import Client
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "fleetdm_vulnerability_logs"
    POINTER_PATH = "date_create"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    def collect(self):
        """Collects all hosts from the FleetDM API.
        """
        client = Client(token=self.key)
        cursor = None

#       We do a full load of hosts on each run as there's no good way to determine if
#       vulns have been added to the system even if the host itself has not updated since
#       last time.

#        # If no pointer is stored then a previous run hasn't been performed, so set the
#        # pointer to a week ago. In the case of the Slack audit API the pointer is the
#        # value of the "date_create" field from the latest record retrieved from the
#        # API - which is in seconds since epoch ("Unix Time") format.
#        try:
#            _ = self.pointer
#        except NotFoundException:
#            self.pointer = (datetime.utcnow() - timedelta(days=7)).strftime("%s")

        # Page over data using the cursor, saving returned data page by page.
        while True:
            log = client.get_hosts(oldest=self.pointer, cursor=cursor)

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

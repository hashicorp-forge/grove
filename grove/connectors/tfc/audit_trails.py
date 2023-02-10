# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Terraform Cloud audit trails connector for Grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.tfc.api import Client
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "tfc_audit_trails"
    POINTER_PATH = "timestamp"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    def collect(self):
        """Collects TFC audit trail from the TFC audit-trails API.

        https://www.terraform.io/docs/cloud/api/audit-trails.html

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(token=self.key)
        cursor = 1

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago. In the case of the TFC audit API the pointer is the
        # value of the "timestamp" field from the latest record retrieved from the
        # API - which is in ISO8601 Format.
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.utcnow() - timedelta(days=7)).isoformat()

        # Get log data from the upstream API.
        while True:
            log = client.get_trails(since=self.pointer, cursor=cursor)

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor  # type: ignore
            if not cursor:
                break

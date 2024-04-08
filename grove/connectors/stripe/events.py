# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Stripe Events connector for Grove."""

from stripe import StripeClient

from grove.connectors import BaseConnector
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "stripe_events"
    POINTER_PATH = "id"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    def collect(self):
        """Collects all events from the Stripe Events API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """

        client = StripeClient(self.key)  # set as env variable
        paging = False

        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = str()

        # Page over data using the cursor, saving returned data page by page.
        while True:
            params = {
                "type": self.operation,
                "limit": 100,
            }
            if self.pointer:
                params["starting_after"] = self.pointer

            if paging:
                entries = entries.next_page()
            else:
                entries = client.events.list(params=params)

            # Save this batch of log entries.
            self.save(entries.list())

            # Check if we need to continue paging.
            paging = entries.has_more
            if not paging:
                break

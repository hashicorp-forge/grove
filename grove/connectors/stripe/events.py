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

        Stripe's list API methods use cursor-based pagination with a unique ID value
        for each event. The Stripe API allows up to 100 read operations per second,
        which is set as the limit in the params object below.
        """
        client = StripeClient(self.key)
        params = {
            "type": self.operation,
            "limit": 100,
        }
        entries = None

        try:
            _ = self.pointer
        except NotFoundException:
            # Stripe does not use a timestamp for filtering, so this will collect as
            # many results as Stripe is willing to give us during the first collection.
            self.pointer = ""

        # Page over data using the cursor, saving returned data page by page.
        while True:
            # If this has not run before, just collect all information Stripe will give
            # us which is 30-days by default.
            if self.pointer:
                params["starting_after"] = self.pointer

            # Pagination is handled a little differently for Stripe due to their SDK,
            # where we call a next_page rather than tracking a cursor.
            if not entries:
                entries = client.events.list(params=params)  # type:ignore
            else:
                entries = entries.next_page()  # type:ignore

            # Save this batch of log entries.
            self.save(entries.data)

            # If Stripe doesn't tell us we have more data, we're complete.
            if not entries.has_more:
                break

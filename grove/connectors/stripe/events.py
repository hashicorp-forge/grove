# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Stripe Events connector for Grove."""

from stripe import StripeClient
from datetime import datetime, timedelta, timezone

from grove.connectors import BaseConnector
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "stripe_events"
    POINTER_PATH = "created"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    def collect(self):
        """Collects all events from the Stripe Events API.

        Stripe's list API methods use cursor-based pagination using the event's created
        parameter. The Stripe API allows up to 100 read operations per second,
        which is set as the limit in the params object below.
        """
        client = StripeClient(self.key)
        params = {
            "type": self.operation,
            "limit": 100,
        }

        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (
                datetime.now(timezone.utc) - timedelta(days=7)
            ).strftime("%s")

        # Page over data using the cursor, saving returned data page by page.
        entries = None
        params["created"] = {"gte": self.pointer}

        while True:
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
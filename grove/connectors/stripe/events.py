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
        for each event.
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

            if self.pointer:
                params["starting_after"] = self.pointer

            if not entries:
                entries = client.events.list(params=params)

            # Save this batch of log entries.
            self.save(list(entries.list()))

            # If the API doesn't return more data then we're finished with
            # this collection.
            if not entries.has_more:
                break

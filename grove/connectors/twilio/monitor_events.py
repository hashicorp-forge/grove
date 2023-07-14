# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Twilio monitor events connector for Grove."""

from datetime import datetime, timedelta, timezone

from twilio.base.exceptions import TwilioException
from twilio.rest import Client

from grove.connectors import BaseConnector
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException, RequestFailedException


class Connector(BaseConnector):
    NAME = "twilio_monitor_events"
    POINTER_PATH = "event_date"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    @property
    def secret(self):
        """Fetches the API secret from the configuration.

        This is required as this is a third authentication element required by Twilio
        when using API authentication.

        :return: The value of the 'secret' field from the configuration.
        """
        try:
            return self.configuration.secret
        except AttributeError:
            return None

    def collect(self):
        """Collects all events from the Twilio Monitor Event API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the 7-days of data will be collected.
        """
        # Construct the client based on whether "API key" or "auth token" authentication
        # is configured.
        if self.secret:
            client = Client(self.key, self.secret, self.identity)
        else:
            client = Client(self.identity, self.key)

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago.
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.now(timezone.utc) - timedelta(days=7)).strftime(
                "%Y-%m-%dT%H:%M:%S%z"
            )

        # Stream the data from the API, paging as required.
        entries = []
        batch_size = 1000

        try:
            events = client.monitor.events.stream(start_date=self.pointer)
        except TwilioException as err:
            raise RequestFailedException(err)

        for event in events:
            # This is less than ideal, but as EventInstance returns datetime objects
            # for the event_date, and does not expose a way to return the entire set
            # of properties we'll need to break encapsulation to get all of the data
            # mapped into the object.
            candidate = event._properties
            candidate["event_date"] = event.event_date.strftime("%Y-%m-%dT%H:%M:%S%z")

            # Track the event.
            entries.append(candidate)

            # Save batches, and clear the list for the next page.
            if len(entries) == batch_size:
                self.save(entries)
                entries = []

        # Save any records not previously saved, before completion.
        self.save(entries)

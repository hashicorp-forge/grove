# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Twilio messages connector for Grove."""

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict

from twilio.base.exceptions import TwilioException
from twilio.rest import Client

from grove.connectors import BaseConnector
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException, RequestFailedException


class Connector(BaseConnector):
    NAME = "twilio_messages"
    POINTER_PATH = "date_sent"
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

    @lru_cache(maxsize=512)  # noqa: B019
    def _carrier_lookup(self, number: str):
        """Performs a Carrier lookup via the Twilio API.

        This is wrapped with an lru_cache to try and reduce lookup costs for retries of
        the same number between collections - assuming the runtime supports long lived
        LRU caches!

        :return: A dictionary of carrier information from Twilio.
        """
        carrier = self.client.lookups.v1.phone_numbers(number).fetch(type=["carrier"])

        # Extract only carrier and country fields, as we don't want to track PII here.
        return {
            "country_code": carrier.country_code,
            "carrier": carrier.carrier,
        }

    def collect(self):  # noqa: C901
        """Collects logs of all messages from the Twilio Messages API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the 24-hours of data will be collected.
        """
        # Construct the client based on whether "API key" or "auth token" authentication
        # is configured. The client is defined as an instance variable so convenience
        # methods can access the same client.
        if self.secret:
            self.client = Client(self.key, self.secret, self.identity)
        else:
            self.client = Client(self.identity, self.key)

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to 24-hours ago.
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
                "%a, %d %b %Y %H:%M:%S %z"
            )

        # Our LRU cache only has 512 slots, so during a back fill this may be exhausted.
        # To try and alleviate we'll track results in memory during a single execution,
        # too.
        carrier_information: Dict[str, Any] = {}

        # Stream the data from the API, paging as required.
        entries = []
        batch_size = 1000

        try:
            events = self.client.messages.list(date_sent=self.pointer)
        except TwilioException as err:
            raise RequestFailedException(err)

        for event in events:
            # Redaction and lookup of numbers will be performed based on the direction
            # of the message.
            incoming = False

            if event.status == "received":
                number = event.from_
                incoming = True
            else:
                number = event.to

            # Check results cache for carrier information before performing a lookup,
            # as these are expensive in terms of both time and cost (currently $0.005
            # per lookup).
            if number in carrier_information:
                carrier = carrier_information[number]
            else:
                carrier = self._carrier_lookup(number)
                carrier_information[number] = carrier

            # Fields need to be removed from the logs to protect message contents and
            # PII, so we'll construct the log messages ourselves.
            #
            # The use of RFC2822 format dates is horrible, but this is the format that
            # this API expects :(
            message = {
                "account_sid": event.account_sid,
                "date_sent": event.date_sent.strftime("%a, %d %b %Y %H:%M:%S %z"),
                "status": event.status,
                "sid": event.sid,
            }

            # Sanitise the message for PII based on direction.
            if incoming:
                message["to"] = event.to
                try:
                    message["from"] = f"{event.from_[0:4]}..."
                except IndexError:
                    message["from"] = "..."
            else:
                message["from"] = event.from_
                try:
                    message["to"] = f"{event.to[0:4]}..."
                except IndexError:
                    message["to"] = "..."

            # Track the message.
            entries.append({**message, **carrier})

            # Save batches, and clear the list for the next page.
            if len(entries) == batch_size:
                self.save(entries)
                entries = []

        # Save any records not previously saved, before completion.
        self.save(entries)

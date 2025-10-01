# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Okta Users connector for Grove."""

from grove.connectors import BaseConnector
from grove.connectors.okta.api import Client
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    CONNECTOR = "okta_users"
    POINTER_PATH = "id"
    LOG_ORDER = CHRONOLOGICAL

    @property
    def domain(self):
        """Fetches the Okta domain from the configuration.

        This field is used to allow configuration of collection of user data from
        specific Okta domains, including okta-emea.com, and oktapreview.com. This must
        not include the customer name / organisation name. The default is 'okta.com'.

        :return: The "domain" portion of the connector's configuration.
        """
        try:
            return self.configuration.domain
        except AttributeError:
            return "okta.com"


    def collect(self):
        """Collects all users from the Okta Users API.

        This will collect all users from the Okta organization. Since user data
        doesn't change frequently, this connector is designed to be run daily to
        capture the current state of all users in the organization.
        
        Grove's built-in frequency mechanism handles when to run this connector.
        """

        client = Client(identity=self.identity, token=self.key, domain=self.domain)
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to an empty string to start from the beginning.
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = ""

        # Get user data from the upstream API, paging if required.
        while True:
            users = client.get_users(cursor=cursor if isinstance(cursor, str) else None)

            # Save this batch of user entries.
            self.save(users.entries)

            # Check if we need to continue paging.
            cursor = users.cursor
            if cursor is None:
                break
            # Ensure cursor is a string for the next iteration
            cursor = str(cursor) if cursor is not None else None


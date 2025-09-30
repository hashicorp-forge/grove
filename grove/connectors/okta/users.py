# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Okta Users connector for Grove."""

from datetime import datetime, timezone

from grove.connectors import BaseConnector
from grove.connectors.okta.api import Client
from grove.constants import CHRONOLOGICAL
from grove.exceptions import ConfigurationException, NotFoundException


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

    @property
    def interval_hours(self):
        """Fetches the minimum interval between collections from the configuration.

        This field controls how often the connector should collect user data. Since user
        data doesn't change frequently, this prevents unnecessary API calls. The default
        is 24 hours (daily collection).

        :return: The minimum hours between collections.
        """
        try:
            candidate = self.configuration.interval_hours
        except AttributeError:
            return 24

        try:
            candidate = int(candidate)
        except ValueError as err:
            raise ConfigurationException(
                f"Configured 'interval_hours' is not valid. Value must be an integer. {err}"
            )

        return candidate

    def collect(self):
        """Collects all users from the Okta Users API.

        This will collect all users from the Okta organization. Since user data
        doesn't change frequently, this connector is designed to be run daily to
        capture the current state of all users in the organization.
        """
        # Check if we should skip collection based on interval
        try:
            last_collection = self.pointer
            if last_collection and last_collection != "":
                # Parse the last collection time from the pointer (stored as ISO timestamp)
                last_time = datetime.fromisoformat(last_collection.replace('Z', '+00:00'))
                time_since_last = datetime.now(timezone.utc) - last_time
                
                if time_since_last.total_seconds() < (self.interval_hours * 3600):
                    self.logger.info(
                        f"Skipping collection - last run was {time_since_last.total_seconds()/3600:.1f} hours ago, "
                        f"interval is {self.interval_hours} hours",
                        extra=self.log_context
                    )
                    return
        except NotFoundException:
            # No pointer exists yet, proceed with collection
            pass
        except (ValueError, TypeError) as err:
            self.logger.warning(
                f"Could not parse last collection time from pointer: {err}. Proceeding with collection.",
                extra=self.log_context
            )

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

        # Update pointer with current timestamp for interval tracking
        self.pointer = datetime.now(timezone.utc).isoformat()

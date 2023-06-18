# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Workday Audit connector for Grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.workday.api import Client
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException

DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class Connector(BaseConnector):
    NAME = "workday_activity_logging"
    POINTER_PATH = "requestTime"
    LOG_ORDER = CHRONOLOGICAL

    @property
    def base_url(self):
        """Fetches the Workday unique base url from the configuration.

        This is required as this is the url required by WD for authentication and API
        usage.

        :return: The "base_url" portion of the connector's configuration.
        """
        try:
            return self.configuration.base_url
        except AttributeError:
            return None

    @property
    def client_id(self):
        """Fetches the Workday Client ID from the configuration.

        This is required as this is an additional authentication element required by WD.

        :return: The "client_id" portion of the connector's configuration.
        """
        try:
            return self.configuration.client_id
        except AttributeError:
            return None

    @property
    def client_secret(self):
        """Fetches the Workday Client Secret from the configuration.

        This is required as this is an additional authentication element required by WD.

        :return: The "client_secret" portion of the connector's configuration.
        """
        try:
            return self.configuration.client_secret
        except AttributeError:
            return None

    def collect(self):
        """Collects all logs from the Workday Audit API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(
            base_url=self.base_url,
            identity=self.identity,
            client_id=self.client_id,
            client_secret=self.client_secret,
            refresh_token=self.key,
        )

        # Use the refresh token to generate a temporary access token.
        client.get_access_token()

        # Set cursor
        cursor = 0

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago.
        now = datetime.utcnow().strftime(DATESTAMP_FORMAT)
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.utcnow() - timedelta(days=7)).strftime(
                DATESTAMP_FORMAT
            )

        # Get log data from the upstream API. A "from" and "to" datetime query
        # parameters are required.
        while True:
            log = client.get_activity_logging(
                from_date=self.pointer,
                to_date=now,
                cursor=cursor,
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor  # type: ignore
            if not cursor:
                break

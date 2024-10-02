# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""FleetDM Vulnerability connector for Grove."""

from datetime import datetime, timedelta, timezone

from grove.connectors import BaseConnector
from grove.connectors.fleetdm.api import Client
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "fleetdm_host_logs"
    POINTER_PATH = "software_updated_at"
    LOG_ORDER = CHRONOLOGICAL

    @property
    def jmespath_queries(self):
        """Fetches the parameters for the jmespath filters of the data response.

        Jmespath query language is defined and can be tested at https://jmespath.org/
        This allows you to configure what data to include or filter from the FleetDM
        response.

        An example is:
            "jmespath_queries": "{hostname:hostname,updated_at:updated_at}"

        This returns the following structure of data about each host:

            {
                updated_at:
                hostname:
            }

        :return: A string of the Jmespath response that should define the JSON object
            to return. Default is *, the full set of JSON response
        """
        try:
            p = self.configuration.jmespath_queries
        except AttributeError:
            return "*"

        return p

    @property
    def params(self):
        """Fetches the parameters for the API call.

        This is used to set what parameters to use in the API call.

        :return: The dict of params defined in the connector configuration.
        """
        try:
            p = self.configuration.params
        except AttributeError:
            return None

        return p

    @property
    def api_uri(self):
        """The URI for the API call

        For example: https://panel.fleetdm.example.com

        :return: The configured FleetDM API URI.
        """
        try:
            p = self.configuration.api_uri
        except AttributeError:
            return None

        return p

    def collect(self):
        """Collects all hosts from the FleetDM API."""
        client = Client(
            token=self.key,
            params=self.params,
            api_uri=self.api_uri,
            jmespath_queries=self.jmespath_queries,
        )

        # We load hosts as they've had their software inventory updated. Grove requires
        # a default pointer to be set, so set the pointer to a week ago. We start at the
        # datetime set by the pointer and loop forward in time until the present.
        try:
            _ = str(datetime.fromisoformat(self.pointer))
        except NotFoundException:
            self.pointer = str(datetime.now(timezone.utc) - timedelta(days=7))

        # Page over data using the cursor, saving returned data page by page.
        while True:
            log = client.get_hosts(
                cursor=self.pointer,
                params=self.params,
                jmespath_queries=self.jmespath_queries,
                api_uri=self.api_uri,
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            if log.cursor is None:
                break

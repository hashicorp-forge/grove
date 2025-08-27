# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""launchdarkly Audit connector for Grove."""

from time import time
from typing import Optional

from grove.connectors import BaseConnector
from grove.connectors.launchdarkly.api import Client
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import ConfigurationException, NotFoundException


class Connector(BaseConnector):
    CONNECTOR = "launchdarkly_audit_records"
    POINTER_PATH = "date"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    @property
    def verbose(self) -> bool:
        """Fetches the verbose option from the configuration.

        :return: The "verbose" portion of the connector's configuration.
        """
        try:
            if not isinstance(self.configuration.verbose, bool):
                raise ConfigurationException("If set, verbose configuration option must be a boolean")
        except AttributeError:
                return False
        return self.configuration.verbose

    def collect(self):
        """Collects launchdarkly audit records from the launchdarkly API.

        If the configuration option "verbose" is false or unset, the collector returns
        the information from the audit log entries list.
        https://launchdarkly.com/docs/api/audit-log/get-audit-log-entries

        If it is set to "true", then it iterates over each log entry and returns the
        detailed information about each entry.
        https://launchdarkly.com/docs/api/audit-log/get-audit-log-entry

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(token=self.key)
        cursor: Optional[str] = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago. In the case of the launchdarkly audit API the pointer is
        # the value of the "date" field from the latest record retrieved from
        # the API.
        try:
            _ = self.pointer
        except NotFoundException:
            since = round((time() * 1000) - 604800000) # Current time minus 7 days in epoch time
            self.pointer = str(since)

        # Get log data from the upstream API, paging as required.
        while True:
            log = client.get_audit_records_list(cursor=cursor, before=None, after=self.pointer, limit="10", verbose=self.verbose)

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = str(log.cursor) if log.cursor is not None else None
            if cursor is None:
                break

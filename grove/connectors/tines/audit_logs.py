# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Tines Audit connector for Grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.tines.api import Client
from grove.constants import OPERATION_DEFAULT, REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "tines_audit_logs"
    POINTER_PATH = "created_at"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    @property
    def domain(self):
        """Fetches the Tines domain suffix from the configuration.

        This field is used to allow configuration of collection of log data from
        specific non 'tines.com' instances. Usually, this will not need to be changed,
        as the configured identity (tenant name) will be appended to this domain to form
        the full FQDN.

        If the required tenant is under 'tines.com', only the usual identity field need
        be set.

        :return: The "domain" portion of the connector's configuration.
        """
        try:
            return self.configuration.domain
        except AttributeError:
            return "tines.com"

    def collect(self):
        """Collects all logs from the Tines Audit API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(
            token=self.key,
            domain=self.domain,
            identity=self.identity,
        )
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago. The Tines API returns timestamps as RFC3339, and
        # without milliseconds.
        now = datetime.utcnow()

        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (now - timedelta(days=7)).isoformat(
                sep="T",
                timespec="seconds",
            ) + "Z"

        # Set the operation name to collect to 'None' if none is specified - as the
        # Grove default is 'all'.
        operation = None

        if self.operation != OPERATION_DEFAULT:
            operation = self.operation

        # Page over data using the cursor, saving returned data page by page.
        while True:
            log = client.list_audit_logs(
                after=self.pointer,
                cursor=cursor,
                operation_name=operation,
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

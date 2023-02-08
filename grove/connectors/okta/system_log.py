"""Okta SystemLog connector for grove."""

from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.okta.api import Client
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "okta_system_log"
    POINTER_PATH = "published"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        """Collects all logs from the Okta Audit API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(identity=self.identity, token=self.key)
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago.
        now = datetime.utcnow()
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (now - timedelta(days=7)).isoformat(
                sep="T",
                timespec="milliseconds",
            ) + "Z"

        # Get log data from the upstream API, paging if required.
        while True:
            log = client.get_audit_logs(since=self.pointer, cursor=cursor)

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

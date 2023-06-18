# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Oomnitza Activities connector for Grove."""
import time
from datetime import datetime, timedelta

from grove.connectors import BaseConnector
from grove.connectors.oomnitza.api import Client
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "oomnitza_activities"
    POINTER_PATH = "timestamp"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    def collect(self):
        """Collects Oomnitza activities from the Oomnitza API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(token=self.key, identity=self.identity)

        # Set cursor
        cursor = 0

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to 2 days ago. In the case of the Oomnitza activities API the pointer
        # is the value of the "timestamp" field from the latest record retrieved from
        # the API - which is in epoch. The Oomnitza API doesnt account for milliseconds.
        now = datetime.fromtimestamp(time.time()).strftime("%s")

        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (
                datetime.fromtimestamp(time.time()) - timedelta(days=2)
            ).strftime("%s")

        # Get log data from the upstream API. A "start_date" and "end_date" datetime
        # query parameters are required.
        while True:
            log = client.get_activites(
                start_date=self.pointer, end_date=now, cursor=cursor
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor  # type: ignore
            if not cursor:
                break

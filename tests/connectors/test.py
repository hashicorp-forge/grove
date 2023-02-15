# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Test connector for Grove."""

import datetime

import requests

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "test_only_connector"
    LOG_ORDER = CHRONOLOGICAL
    POINTER_PATH = "timestamp"

    def collect(self):
        """Collects all logs."""

        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = str(int(datetime.datetime.now().timestamp()))

        while True:
            response = requests.get(
                "http://192.0.2.1",
                timeout=1,
                verify=False,
                params={
                    "start_time": self.pointer,
                },
            )
            entries = response.json().get("logs", [])
            cursor = response.json().get("cursor")

            # Save this batch of log entries.
            self.save(entries)

            # Check if we need to continue paging.
            if cursor is None:
                break

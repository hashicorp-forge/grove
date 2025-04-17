# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zitadel Events connector for Grove."""

import time
from typing import Any, Dict, List, Optional

import requests

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import (
    NotFoundException,
    RateLimitException,
    RequestFailedException,
)


class Connector(BaseConnector):
    CONNECTOR = "zitadel_events_aggregate_user"
    POINTER_PATH = "sequence"
    LOG_ORDER = CHRONOLOGICAL
    RATE_LIMIT_WINDOW = 60

    @property
    def batch_size(self) -> int:
        """Fetches the batch size from the configuration.

        :return: The "batch_size" portion of the connector's configuration.
        """
        try:
            return self.configuration.batch_size
        except AttributeError:
            return 10

    @property
    def timeout(self) -> int:
        """Fetches the request timeout from the configuration - in seconds.

        :return: The "timeout" portion of the connector's configuration.
        """
        try:
            return self.configuration.timeout
        except AttributeError:
            return 100

    @property
    def aggregate_event_types(self) -> List[str]:
        """Fetches a list of aggregate event types from the configuration.

        :return: A list of "aggregate_event_types" from the connector's configuration.
        """
        try:
            return self.configuration.aggregate_event_types
        except AttributeError:
            return []

    def _build_query(self, last_sequence: Optional[str] = None) -> Dict[str, Any]:
        """Convenience method to construct a Zitadel API request body.

        :param last_sequence: An optional sequence number to query events after.

        :return: A dictionary expressing a Zitadel API request body.
        """
        query: Dict[str, Any] = {
            "limit": self.batch_size,
            "asc": True,
        }

        if self.aggregate_event_types:
            query["aggregateTypes"] = self.aggregate_event_types

        if last_sequence:
            query["sequence"] = last_sequence

        return query

    def _make_request(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convenience method to perform an HTTP request to collect events.

        :param query: A set of parameters to be provided as the query to the API.

        :return: The results from the Zitadel API for the provided query.
        """
        host = self.configuration.identity.rstrip("/")
        url = f"{host}/admin/v1/events/_search"
        retries = 5
        attempts = 0

        while True:
            try:
                response = requests.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {self.configuration.key}",
                        "Content-Type": "application/json",
                    },
                    json=query,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError as err:
                if response.status_code == 429:
                    if attempts < retries:
                        time.sleep(
                            int(
                                response.headers.get(
                                    "Retry-After",
                                    self.RATE_LIMIT_WINDOW,
                                )
                            )
                        )
                        attempts += 1
                    else:
                        raise RateLimitException(err)

                    continue

                raise RequestFailedException(f"Request failed: {err}")

        try:
            _ = response.json()
        except requests.exceptions.JSONDecodeError:
            return None

        return response.json()

    def collect(self):
        """Collects events from the Zitadel API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, all data will be collected.
        """
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = "0"

        has_more = True

        while has_more:
            query = self._build_query(self.pointer)
            result = self._make_request(query)
            events = result.get("events", [])
            if not events:
                break
            self.save(events)

            has_more = result.get("pagination", {}).get("has_more", False)

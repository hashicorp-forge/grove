# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zitadel Events connector for Grove."""

import time
from typing import Any, Dict, Optional

import requests
from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException, RequestFailedException


# Zitadel Events API allows you to retrieve all events based on a given aggregate event type


class Connector(BaseConnector):
    CONNECTOR = "zitadel_events_aggregate_user"
    POINTER_PATH = "sequence"
    LOG_ORDER = CHRONOLOGICAL
    RATE_LIMIT_WINDOW = 60

    def configure(self):
        self._host = self.configuration.identity
        self._pat = self.configuration.key

        self._batch_size = self.configuration.batch_size
        self._timeout = self.configuration.timeout
        self._event_types = self.configuration.aggregate_event_types

    def _build_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._pat}",
            "Content-Type": "application/json",
        }

    def _build_query(self, last_sequence: Optional[str] = None) -> Dict[str, Any]:
        query = {
            "limit": self._batch_size,
            "asc": True,
        }

        if self._event_types:
            query["aggregateTypes"] = self._event_types
            
        if last_sequence:
            query["sequence"] = last_sequence

        return query

    def _make_request(self, query: dict) -> dict:
        url = f"{self._host.rstrip('/')}/admin/v1/events/_search"
    
        try:
            response = requests.post(
                url,
                headers=self._build_headers(),
                json=query,
                timeout=self._timeout,
            )
            response.raise_for_status()

        except requests.exceptions.HTTPError as err:
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", self.RATE_LIMIT_WINDOW))
                time.sleep(retry_after)
                return self._make_request(query)
            raise RequestFailedException(f"Request failed: {err}")

        try:
            _ = response.json()  
        except requests.exceptions.JSONDecodeError:
            return None

        return response.json()  


    def collect(self):
        self.configure()  

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


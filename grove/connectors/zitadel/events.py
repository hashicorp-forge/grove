# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zitadel Events connector for Grove."""

import json
import time
from typing import Any, Dict, Optional

import requests
from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import ConfigurationException, NotFoundException, RequestFailedException


# Zitadel Events API allows you to retrieve all events based on a given aggregate event type


class Connector(BaseConnector):
    CONNECTOR = "zitadel_events_aggregate_user"
    POINTER_PATH = "sequence"
    LOG_ORDER = CHRONOLOGICAL
    RATE_LIMIT_WINDOW = 60

    def configure(self):
        self._host = self.configuration.identity
        if not self._host:
            raise ConfigurationException("Missing required 'identity' parameter")
        
        self._pat = self.configuration.key
        if not self._pat:
            raise ConfigurationException("Missing required 'key' parameter")

        self._batch_size = self.configuration.batch_size
        self._timeout = self.configuration.timeout
        self._event_types = self.configuration.aggregate_event_types

    def _build_headers(self) -> dict:
        print('this is the pat', self._pat)
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
            print("Request URL:", url)
            print("Request Headers:", json.dumps(self._build_headers(), indent=2))
            print("Request Body:", json.dumps(query, indent=2))
            response = requests.post(
                url,
                headers=self._build_headers(),
                json=query,
                timeout=self._timeout,
            )
            print("Response status code:", response.status_code)  # Prints 200
            print("Response text:", response.text)  # Prints raw response content
            response.raise_for_status()

            # If the response is JSON, parse it
            try:
                response_data = response.json()
                print("Response JSON:", response_data)
            except requests.exceptions.JSONDecodeError:
                print("Response is not JSON.")

        except requests.exceptions.HTTPError as err:
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", self.RATE_LIMIT_WINDOW))
                time.sleep(retry_after)
                return self._make_request(query)
            raise RequestFailedException(f"Request failed: {err}")

        return response.json()

    def collect(self):
        self.configure()  # Ensure configuration is set up before collecting

        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = "0"

        cursor = str(self.pointer) if self.pointer else None
        has_more = True

        while has_more:
            try:
                query = self._build_query(cursor)
                result = self._make_request(query)
                
                events = result.get("events", [])
   
                if not events:
                    print("no new events found")
                    break

                self.save(events)
                cursor = max(cursor or "0", max(event.get("sequence", "0") for event in events))
                self.pointer = cursor
                has_more = result.get("pagination", {}).get("has_more", False)
                
            except RequestFailedException as err:
                break

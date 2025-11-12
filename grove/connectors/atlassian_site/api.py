# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Atlassian Site audit API client.
"""

import base64
import logging
import time
from typing import Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://{identity}.atlassian.net/rest/api/3/auditing/record"
API_PAGE_SIZE = 100


class Client:
    def __init__(
        self,
        identity: Optional[str] = None,
        username: Optional[str] = None,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new client.

        :param identity: The Atlassian site name
        :param token: The Atlassian API token.
        :param username: The allocated atlassian username
        :param retry: Whether to automatically retry if recoverable errors are
            encountered, such as rate-limiting.
        """
        self.identity = identity
        self.retry = retry
        creds = base64.b64encode(f"{username}:{token}".encode())
        self.logger = logging.getLogger(__name__)

        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Basic {creds.decode()}",
        }
        self._api_base_uri = API_BASE_URI.format(identity=identity)

    def _get(
        self, url: str, params: Optional[Dict[str, Optional[str]]] = None
    ) -> HTTPResponse:
        """A GET wrapper to handle retries for the caller.

        :param url: A URL to perform the HTTP GET against.
        :param parameters: An optional set of HTTP parameters to add to the request.

        :raises RateLimitException: A rate limit was encountered.
        :raises RequestFailedException: An HTTP request failed.

        :return: HTTP Response object containing the headers and body of a response.
        """
        while True:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as err:
                # Retry on rate-limit, but only if requested.
                if getattr(err.response, "status_code", None) == 429:
                    self.logger.warning("Rate-limit was exceeded during request")
                    if self.retry:
                        time.sleep(int(err.response.headers.get("Retry-After", "1")))
                        continue
                    else:
                        raise RateLimitException(err)

                raise RequestFailedException(err)

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_audit(
        self,
        cursor: int = 0,
        to_date: Optional[str] = None,
        from_date: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit logs which match the provided filters.

        :param from_date: The required date and time of the earliest log entry.
        :param to_date: The required date and time in UTC of the latest log entry.
        :param limit: The maximum number of items to include in a single response.
        :param cursor: The index position of the first object in a response collection.
            Cursor to use when fetching events (pagination).

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        # The endpoint returns the same total value of results regardless of the limit
        # and offset parameters. The pagination parameters determine the amount of
        # content in the data[] array.
        result = self._get(
            f"{self._api_base_uri}",
            params={
                "from": from_date,
                "to": to_date,
                "limit": str(API_PAGE_SIZE),
                "offset": str(cursor),
            },
        )

        data = result.body.get("records", [])

        # keep paging until we meet the total number of results
        if len(data) == API_PAGE_SIZE:
            cursor += API_PAGE_SIZE
        else:
            cursor = 0

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=data)

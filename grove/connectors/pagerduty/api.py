# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""PagerDuty Audit API client."""

import logging
import time
from typing import Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://api.pagerduty.com"


class Client:
    def __init__(
        self,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new client.

        :param token: The PagerDuty API Key.
        :param retry: Whether to automatically retry if recoverable errors are
            encountered, such as rate-limiting.
        """
        self.retry = retry
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Accept": "application/vnd.pagerduty+json;version=3",
            "Authorization": f"Token token={token}",
            "Content-Type": "application/json",
        }

    def _get(
        self,
        url: str,
        params: Optional[Dict[str, Optional[str]]] = None,
    ) -> HTTPResponse:
        """A GET wrapper to handle retries for the caller.

        :param url: URL to perform the HTTP GET against.
        :param params: HTTP parameters to add to the request.

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
                        time.sleep(1)
                        continue
                    else:
                        raise RateLimitException(err)

                raise RequestFailedException(err)

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_records(
        self,
        since: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit logs which match the provided filters.

        :param since: The ISO8601 format of the most recent event to fetch (inclusive).
        :param cursor: The cursor to use when paging.
        :param limit: Number of audit records per request.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        # See psf/requests issue #2651 for why we can happily pass in None values
        # and not have the request key added to the URI.
        result = self._get(
            f"{API_BASE_URI}/audit/records",
            params={
                "since": since,
                "cursor": cursor,
                "limit": limit,
            },
        )

        # Record the cursor, if set.
        cursor = result.body.get("next_cursor", None)

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body.get("records", []))

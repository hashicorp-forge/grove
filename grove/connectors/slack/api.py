# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Slack Audit API client.

The official Slack SDK does not currently support the Audit API, this client has been
created in the interim.
"""

import logging
import time
from typing import Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://api.slack.com/audit/v1"


class Client:
    def __init__(
        self,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new Slack audit API client.

        :param token: Slack API Bearer token.
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        self.retry = retry
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
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
                        time.sleep(int(err.response.headers.get("Retry-After", "1")))
                        continue
                    else:
                        raise RateLimitException(err)

                raise RequestFailedException(err)

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_logs(
        self,
        latest: Optional[str] = None,
        oldest: Optional[str] = None,
        action: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit logs which match the provided filters.

        :param latest: Unix timestamp of the most recent event to include (inclusive).
        :param oldest: Unix timestamp of the least recent event to include (inclusive).
        :param action: Name of the action to request events for.
        :param cursor: Cursor to use when fetching events (pagination).

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        # See psf/requests issue #2651 for why we can happily pass in None values and
        # not have the request key added to the URI.
        result = self._get(
            f"{API_BASE_URI}/logs",
            params={
                "latest": latest,
                "oldest": oldest,
                "action": action,
                "cursor": cursor,
                "limit": "1000",
            },
        )

        # Slack appears to return an empty string for a cursor if there isn't one, so
        # swap this for None in this case to avoid having to rely on "falsy" conditions.
        cursor = result.body.get("response_metadata", {}).get("next_cursor", None)
        if cursor == "":
            cursor = None

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body.get("entries", []))

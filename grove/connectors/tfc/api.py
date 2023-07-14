# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Terraform Cloud audit trail API client.

The official TFC SDK does not currently support the Audit API, so interactions need
to be handled manually.
"""

import logging
import time
from typing import Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://app.terraform.io/api/v2"


class Client:
    def __init__(
        self,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ) -> None:
        """Setup a new client.

        :param token: The TFC API Bearer token.
        :param retry: Whether to automatically retry if recoverable errors are
            encountered, such as rate-limiting.
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
                        time.sleep(1)
                        continue
                    else:
                        raise RateLimitException(err)

                raise RequestFailedException(err)

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_trails(
        self,
        since: Optional[str] = None,
        cursor: Optional[int] = 1,
        page_size: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit logs which match the provided filters.

        :param since: The ISO8601 date of the most recent event to include (inclusive).
        :param cursor: The page to fetch. If omitted, endpoint returns first page.
        :param page_size: Number of audit events per page. Defaults to 1000.

        :return: AuditLogEntries object containing a pagination cursor, and log
            entries.
        """
        # See psf/requests issue #2651 for why we can happily pass in None values
        # and not have the request key added to the URI.
        result = self._get(
            f"{API_BASE_URI}/organization/audit-trail",
            params={
                "since": since,
                "page[number]": str(cursor),
                "page[size]": page_size,
            },
        )
        cursor = result.body.get("pagination", {}).get("next_page", 0)

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body.get("data", []))

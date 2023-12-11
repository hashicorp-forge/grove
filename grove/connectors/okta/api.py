# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Okta API client."""

import datetime
import logging
import time
from typing import Dict, Optional
from urllib.parse import unquote

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://{identity}.{domain}"
API_PAGE_SIZE = 1000


class Client:
    def __init__(
        self,
        domain: str = "okta.com",
        identity: Optional[str] = None,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new client.

        :param identity: The Okta subdomain.
        :param domain: The Okta domain, without customer subdomain.
        :param token: The Okta API key.
        :param retry: Whether to automatically retry if recoverable errors are
            encountered, such as rate-limiting.
        """
        self.retry = retry
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"SSWS {token}",
        }

        self._api_base_uri = API_BASE_URI.format(identity=identity, domain=domain)

    def _parse_link_header(self, link: str) -> str:
        """Attempts to parse a URL from the provided Link header.

        :param link: The value of a Link header returned from a previous request.

        :raises ValueError: No, or an invalid, Link header was encountered.

        :return: A clean and ready to use URL from the Link header provided by Okta.
        """
        # A link header may contain N entries ("next" and "self").
        url = None
        next_url = None
        self_url = None
        links = link.split(",")

        for entry in links:
            link_parts = entry.split(";")
            link_rel = link_parts[-1].strip()

            if "next" in link_rel:
                next_url = unquote(link_parts[0].strip().lstrip("<").rstrip(">"))

            elif "self" in link_rel:
                self_url = unquote(link_parts[0].strip().lstrip("<").rstrip(">"))

        # If self_url and next_url are equal than this is the last page, return a
        # value error, otherwise return the next page url.
        if not next_url or next_url == self_url:
            raise ValueError()

        # Try to mitigate SSRFs where a baked Link header is returned.
        url = next_url

        if self._api_base_uri.lower() not in url.lower():
            raise ValueError(
                f"{self._api_base_uri} not found in Link header ({url}). Ignoring."
            )

        return url

    def _get(
        self,
        url: str,
        params: Optional[Dict[str, Optional[str]]] = None,
    ) -> HTTPResponse:
        """A GET wrapper to handle pagination for the caller.

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
                # 429s are used with a header to indicate rate-limit exceeded.
                if getattr(err.response, "status_code", None) != 429:
                    raise RequestFailedException(err)

                if err.response.headers.get("X-Rate-Limit-Remaining") != "0":
                    raise RequestFailedException(err)

                # Retry on rate-limit, but only if requested.
                self.logger.warning("Rate-limit was exceeded during request")
                if self.retry:
                    time_current = int(
                        datetime.datetime.now(datetime.timezone.utc).strftime("%s")
                    )
                    time_ratelimit_reset = int(
                        err.response.headers.get("X-Rate-Limit-Reset", time_current)
                    )

                    # If there was no X-Rate-Limit-Reset header, or the time is in
                    # the past, just wait for a second before retry.
                    if time_ratelimit_reset > time_ratelimit_reset:
                        time.sleep(time_ratelimit_reset - time_current)
                    else:
                        time.sleep(1)

                    continue
                else:
                    raise RateLimitException(err)

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_audit_logs(
        self,
        since: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> AuditLogEntries:
        """Get log data from the upstream API.

        The Okta system log API uses Polling Requests and Bounded Requests and has a
        request limit of 1000 per minute:

            https://developer.okta.com/docs/reference/api/system-log/#request-parameters

        :param since: Filters the upper time bound of the log events published
            property for bounded queries or persistence time for polling queries.
        :param cursor: Cursor to use when fetching results. Supersedes other
            parameters.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        # Use the cursor URL if set, otherwise construct the initial query.
        if cursor is not None:
            self.logger.debug(
                "Collecting next page using Okta provided cursor",
                extra={"cursor": cursor},
            )
            result = self._get(cursor)
        else:
            result = self._get(
                f"{self._api_base_uri}/api/v1/logs",
                params={
                    "since": since,
                    "sortOrder": "ASCENDING",
                    "limit": str(API_PAGE_SIZE),
                },
            )

        # Track the results.
        try:
            cursor = self._parse_link_header(result.headers.get("Link", ""))
        except ValueError:
            cursor = None

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body)  # type: ignore

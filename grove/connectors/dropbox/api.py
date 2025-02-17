# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Dropbox API client."""

import logging
import time
from typing import Any, Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_HOSTNAME = "api.dropboxapi.com"
API_PAGE_SIZE = 1000


class Client:
    def __init__(
        self,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new Dropbox team events client.

        :param token: Dropbox access token token to authenticate with.
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        self.retry = retry
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _post(
        self,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Optional[str]]] = None,
    ) -> HTTPResponse:
        """A POST wrapper to handle retries for the caller.

        :param url: URL to perform the HTTP POST against.
        :param payload: Dictionary of data to pass as JSON in the request.
        :param params: HTTP parameters to add to the request.

        :raises RateLimitException: A rate limit was encountered.
        :raises RequestFailedException: An HTTP request failed.

        :return: HTTP Response object containing the headers and body of a response.
        """
        while True:
            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers=self.headers,
                    params=params,
                )
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as err:
                if int(getattr(err.response, "status_code", 0)) != 429:
                    raise RequestFailedException(err)

                # Retry on rate-limit, but only if requested.
                self.logger.warning("Rate-limit was exceeded during request")
                if not self.retry:
                    raise RateLimitException(err)

                # If the rate-limit retry is greater than a few of minutes, just bail as
                # we'll pick back up at the next execution.
                time_wait = int(err.response.headers.get("Retry-After", 1))
                if time_wait >= 180:
                    raise RateLimitException(err)

                # Only wait for a second if the retry time was unset or in the past.
                if time_wait > 0:
                    time.sleep(time_wait)
                else:
                    time.sleep(1)

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_events(
        self,
        cursor: Optional[str] = None,
        start_time: Optional[str] = None,
        category: Optional[str] = None,
    ) -> AuditLogEntries:
        """Returns a list of team events.

        :param cursor: Cursor to use when fetching results. Supersedes other parameters.
        :param start_time: The ISO Format timestamp to query logs since.
        :param category: An optional category to collect events for. If not specified
            logs will be collected for all categories.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        url = f"https://{API_HOSTNAME}/2/team_log/get_events"

        # Use the cursor URL if set, otherwise construct the initial query.
        if cursor is not None:
            url = f"{url}/continue"

            self.logger.debug(
                "Collecting next page with provided cursor",
                extra={
                    "cursor": cursor,
                },
            )
            result = self._post(url, payload={"cursor": cursor})
        else:
            # See psf/requests issue #2651 for why we can happily pass in None values
            # and not have the request key added to the URI.
            result = self._post(
                url,
                payload={
                    "category": category,
                    "limit": API_PAGE_SIZE,
                    "start_time": start_time,
                },
            )

        # Check if pagination is required.
        if result.body.get("has_more", False):
            cursor = result.body.get("cursor")
        else:
            cursor = None

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body.get("events", []))

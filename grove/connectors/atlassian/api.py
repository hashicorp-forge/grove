# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Atlassian API client.

As Atlassian does not currently support the Events API, this client has been created in
the interim.
"""

import datetime
import logging
import time
from typing import Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://api.atlassian.com/admin/v1/orgs/{identity}"


class Client:
    def __init__(
        self,
        identity: Optional[str] = None,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new client.

        :param identity: The name of the Atlassian organisation.
        :param token: The Atlassian API token.
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        self.logger = logging.getLogger(__name__)
        self.retry = retry
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        # We need to push the identity into the URI, so we'll keep track of this.
        self._api_base_uri = API_BASE_URI.format(identity=identity)

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
                if int(getattr(err.response, "status_code", 0)) not in [429]:
                    raise RequestFailedException(err)

                if err.response.headers.get("X-Ratelimit-Remaining") != "0":
                    raise RequestFailedException(err)

                # Retry on rate-limit, but only if requested.
                self.logger.warning(
                    "Rate-limit was exceeded during request",
                    extra={
                        "Reset-At": err.response.headers.get("X-Ratelimit-Reset"),
                        "Limit": err.response.headers.get("X-Ratelimit-Limit"),
                    }
                )
                if not self.retry:
                    raise RateLimitException(err)

                time_current = int(
                    datetime.datetime.now(datetime.timezone.utc).strftime("%s")
                )
                time_ratelimit_reset = int(
                    err.response.headers.get("X-Ratelimit-Reset", time_current)
                )
                time_wait = time_ratelimit_reset - time_current

                # If the rate-limit retry is greater than a few of minutes, just bail as
                # we'll pick back up at the next execution.
                if time_wait >= 180:
                    raise RateLimitException(err)

                # Only wait for a second if the retry time was unset or in the past.
                if time_wait > 0:
                    time.sleep(time_wait)
                else:
                    time.sleep(1)

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_audit(
        self,
        cursor: Optional[str] = None,
        from_date: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of signing attempt logs.

        :param cursor: Cursor to use when fetching results. Supersedes other parameters.
        :param from_date: The earliest date an event represented as a UNIX epoch time.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        url = f"{self._api_base_uri}/events"

        # Use the cursor if set, otherwise construct the initial query.
        if cursor is not None:
            self.logger.debug(
                "Collecting next page with provided cursor.", extra={"cursor": cursor}
            )
            result = self._get(url, params={"from": from_date, "cursor": cursor})
        else:
            self.logger.debug(
                "Collecting first page with provided", extra={"from_date": from_date}
            )
            result = self._get(url, params={"from": from_date})

        cursor = result.body.get("meta", {}).get("next", None)

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body.get("data", []))

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""1Password Audit API client."""

import logging
import time
from typing import Any, Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_HOSTNAME = "events.{domain}"  # Domain will be inserted from config
API_DEFAULT_DOMAIN = "1password.com"  # Default to US endpoint
API_PAGE_SIZE = 1000


class Client:
    def __init__(
        self,
        domain: Optional[str] = API_DEFAULT_DOMAIN,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new 1Password audit log client.

        :param domain: Domain suffix for the 1Password API ('1password.com' for US, '1password.eu' for EU).
                       Defaults to '1password.com' (US endpoint).
        :param token: 1Password token to authenticate with.
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        self.retry = retry
        self.hostname = API_HOSTNAME.format(domain=domain or API_DEFAULT_DOMAIN)
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

    def get_events(
        self,
        event_type: str,
        cursor: Optional[str] = None,
        start_time: Optional[str] = None,
    ) -> tuple[AuditLogEntries, bool]:
        """Returns a list of logs from a specified endpoint.

        :param event_type: The API endpoint name and type of logs we're pulling.
        :param cursor: Cursor to use when fetching results. Supersedes other parameters.
        :param start_time: The ISO Format timestamp to query logs since.

        :return:
            - AuditLogEntries object containing a pagination cursor, and log entries.
            - Boolean indicating whether there are more events to process.
        """
        url = f"https://{self.hostname}/api/v2/{event_type}"

        # Use the cursor URL if set, otherwise construct the initial query.
        if cursor is not None:
            self.logger.debug(
                "Collecting next page with provided cursor",
                extra={"cursor": cursor},
            )
            result = self._post(url, payload={"cursor": cursor})
        else:
            # See psf/requests issue #2651 for why we can happily pass in None values
            # and not have the request key added to the URI.
            result = self._post(
                url,
                payload={"limit": API_PAGE_SIZE, "start_time": start_time},
            )

        cursor = result.body.get("cursor")
        has_more = result.body.get("has_more", False)

        # Return the cursor and the results to allow the caller to page as required.
        return (
            AuditLogEntries(cursor=cursor, entries=result.body.get("items", [])),
            has_more,
        )

    def get_signinattempts(
        self,
        cursor: Optional[str] = None,
        start_time: Optional[str] = None,
    ) -> tuple[AuditLogEntries, bool]:
        """Fetches a list of signing attempt logs.

        :param cursor: Cursor to use when fetching results. Supersedes other parameters.
        :param start_time: The ISO Format timestamp to query logs since.

        :return:
            - AuditLogEntries object containing a pagination cursor, and log entries.
            - Boolean indicating whether there are more events to process.
        """
        events, has_more = self.get_events(
            event_type="signinattempts", cursor=cursor, start_time=start_time
        )

        # when saving the pointer, Grove looks for the pointer value within an entry
        # rather than externally to it. 1Password's pointer is external to the entry. To
        # get around this, copy internally.
        if len(events.entries) > 0:
            events.entries[-1]["cursor"] = events.cursor

        return events, has_more

    def get_itemusages(
        self,
        cursor: Optional[str] = None,
        start_time: Optional[str] = None,
    ) -> tuple[AuditLogEntries, bool]:
        """Fetches a list of modified, accessed, or used items from a shared vault.

        :param cursor: Cursor to use when fetching results. Supersedes other parameters.
        :param start_time: The ISO Format timestamp to query logs since.

        :return:
            - AuditLogEntries object containing a pagination cursor, and log entries.
            - Boolean indicating whether there are more events to process.
        """
        events, has_more = self.get_events(
            event_type="itemusages", cursor=cursor, start_time=start_time
        )

        # when saving the pointer, Grove looks for the pointer value within an entry
        # rather than externally to it. 1Password's pointer is external to the entry. To
        # get around this, copy internally.
        if len(events.entries) > 0:
            events.entries[-1]["cursor"] = events.cursor

        return events, has_more

    def get_auditevents(
        self,
        cursor: Optional[str] = None,
        start_time: Optional[str] = None,
    ) -> tuple[AuditLogEntries, bool]:
        """Fetches a list of actions performed by members of a 1Password account.

        :param cursor: Cursor to use when fetching results. Supersedes other parameters.
        :param start_time: The ISO Format timestamp to query logs since.

        :return:
            - AuditLogEntries object containing a pagination cursor, and log entries.
            - Boolean indicating whether there are more events to process.
        """
        events, has_more = self.get_events(
            event_type="auditevents", cursor=cursor, start_time=start_time
        )

        # when saving the pointer, Grove looks for the pointer value within an entry
        # rather than externally to it. 1Password's pointer is external to the entry. To
        # get around this, copy internally.
        if len(events.entries) > 0:
            events.entries[-1]["cursor"] = events.cursor

        return events, has_more

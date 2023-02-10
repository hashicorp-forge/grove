# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Atlassian API client.

As Atlassian does not currently support the Events API, this client has been created in
the interim.
"""

import logging
from typing import Dict, Optional

import requests

from grove.exceptions import RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://api.atlassian.com/admin/v1/orgs/{identity}"


class Client:
    def __init__(
        self,
        identity: Optional[str] = None,
        token: Optional[str] = None,
    ):
        """Setup a new client.

        :param identity: The name of the Atlassian organisation.
        :param token: The Atlassian API token.
        """
        self.logger = logging.getLogger(__name__)
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

        :raises RequestFailedException: An HTTP request failed.

        :return: HTTP Response object containing the headers and body of a response.
        """
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            raise RequestFailedException(err)

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

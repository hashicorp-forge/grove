# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Workday API client.

As the Python Workday client is no longer maintained this client has been created to
allow collection of log data.
"""

import logging
import time
from typing import Any, Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://{base_url}/ccx/api/privacy/v1/{identity}"
API_PAGE_SIZE = 100


class Client:
    def __init__(
        self,
        base_url: Optional[str] = None,
        identity: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new client.

        :param base_url: The Workday instance base url
        :param identity: The Workday tenant name
        :param client_id: The Workday integration client id
        :param client_secret: The Workday integration client secret
        :param refresh_token: The allocated Workday integration client refresh token
        :param retry: Whether to automatically retry if recoverable errors are
            encountered, such as rate-limiting.
        """
        self.base_url = base_url
        self.identity = identity
        self.client_secret = client_secret
        self.client_id = client_id
        self.refresh_token = refresh_token
        self.retry = retry
        self.logger = logging.getLogger(__name__)

        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self._api_base_uri = API_BASE_URI.format(base_url=base_url, identity=identity)

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

    def _post(
        self,
        url: str,
        headers: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Optional[str]]] = None,
    ) -> Dict[str, Any]:
        """A POST wrapper to handle retries for the caller.

        :param url: URL to perform the HTTP POST against.
        :param headers: Dictionary of headers to add to the request.
        :param data: HTTP parameters to add to the request.

        :raises RequestFailedException: An HTTP request failed.

        :return: The response to the request in JSON.
        """
        try:
            response = requests.post(
                url,
                headers=headers,
                data=data,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            raise RequestFailedException(err)

        return response.json()

    def get_access_token(self):
        """Exchange a refresh token for an access token.

        This is required by Workday to auth the integration and then grant the bearer
        token to access the API.

        :return: If the request is successful, the bearer token is returned to
            the Client class header.
        """
        url = f"https://{self.base_url}/ccx/oauth2/{self.identity}/token"

        bearer_response = self._post(
            url,
            data={
                "client_id": f"{self.client_id}",
                "client_secret": f"{self.client_secret}",
                "grant_type": "refresh_token",
                "refresh_token": f"{self.refresh_token}",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        access_token = bearer_response.get("access_token")
        self.headers["Authorization"] = f"Bearer {access_token}"

    def get_activity_logging(
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
            f"{self._api_base_uri}/activityLogging",
            params={
                "from": from_date,
                "to": to_date,
                "instancesReturned": "3",
                "limit": str(API_PAGE_SIZE),
                "offset": str(cursor),
            },
        )

        data = result.body.get("data", [])

        # keep paging until we meet the total number of results
        if len(data) == API_PAGE_SIZE:
            cursor += API_PAGE_SIZE
        else:
            cursor = 0

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=data)

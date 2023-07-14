# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zoom API client.

As the Python Zoom client does not currently support Audit API, this client has been
created in the interim.
"""

import base64
import logging
import time
from typing import Any, Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://api.zoom.us"


class Client:
    def __init__(
        self,
        identity: Optional[str] = None,
        client_id: Optional[str] = None,
        key: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new client.

        :param identity: The Zoom account ID
        :param client_id: The Zoom integration client id
        :param key: The Zoom integration client secret
        :param access_token: The allocated Zoom integration client refresh token
        :param retry: Whether to automatically retry if recoverable errors are
            encountered, such as rate-limiting.
        """
        self.identity = identity
        self.key = key
        self.client_id = client_id
        self.retry = retry
        self.logger = logging.getLogger(__name__)

        self.headers = {
            "Content-Type": "application/json",
        }

    def _get(
        self, url: str, params: Optional[Dict[str, Optional[str]]] = None
    ) -> HTTPResponse:
        """A GET wrapper to handle retries for the caller.

        :param url: A URL to perform the HTTP GET against.
        :param headers: A dictionary of headers to add to the request.
        :param parameters: An optional set of HTTP parameters to add to the request.

        :return: HTTP Response object containing the headers and body of a response.

        :raises RateLimitException: A rate limit was encountered.
        :raises RequestFailedException: An HTTP request failed.
        """
        while True:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError as err:
                # Retry on rate-limit, but only if requested.
                if err.response.status_code == 429:
                    self.logger.warning("Rate-limit was exceeded during request")
                    if self.retry:
                        time.sleep(int(err.response.headers.get("Retry-After", "1")))
                        continue
                    else:
                        raise RateLimitException(err) from err

                raise RequestFailedException(err) from err

        return HTTPResponse(headers=response.headers, body=response.json())

    def _post(
        self,
        url: str,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """A POST wrapper to handle retries for the caller.

        :return: the json response.

        :raises RequestFailedException: An HTTP request failed.
        """
        try:
            response = requests.post(
                url,
                headers=headers,
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise RequestFailedException(err) from err

        return response.json()

    def get_access_token(self):
        """Use Basic Auth to get Bearer Token.

        This is required by Zoom to auth the integration and then grant
        the bearer token to access the API.

        To get the access token the accountid, grant_type has to be in the url and
        not in the data.

        :returns: If the request is successful, the bearer token is returned to
            the Client class header.
        """
        grant_type = "account_credentials"
        url = f"https://zoom.us/oauth/token?grant_type={grant_type}&account_id={self.identity}"

        # set basic auth value for authorization header
        basic_auth = str(
            base64.b64encode(bytes(f"{self.client_id}:{self.key}", "utf-8")),
            "utf-8",
        )

        bearer_response = self._post(
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Basic {basic_auth}",
            },
        )

        access_token = bearer_response.get("access_token")
        self.headers["Authorization"] = f"Bearer {access_token}"

    def get_logs(
        self,
        endpoint: str,
        result_field: str,
        to_date: Optional[str] = None,
        from_date: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of logs from Zoom which match the provided filters.

        :param from_date: The required date of the earliest log entry.
        :param to_date: The required date of the latest log entry.
        :param limit: The maximum number of items to include in a single response.
        :param cursor: The 'next_page_token' returned from a request.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        # The endpoint returns the same total value of results regardless of the limit
        # and offset parameters. The pagination parameters determine the amount of
        # content in the data[] array.

        result = self._get(
            f"{API_BASE_URI}/{endpoint}",
            params={
                "from": from_date,
                "to": to_date,
                "next_page_token": cursor,
            },
        )

        # get the list of logs from the result_field parameters value
        data = result.body.get(result_field, [])

        # keep paging until we meet the total number of results
        cursor = result.body.get("next_page_token", None)
        if cursor == "":
            cursor = None

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=data)

    def get_operationlogs(
        self,
        from_date: Optional[str],
        to_date: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit logs from Zoom which match the provided filters.

        :param from_date: The required date of the earliest log entry.
        :param to_date: The required date of the latest log entry.
        :param cursor: The cursor to use when paging.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        return self.get_logs(
            endpoint="v2/report/operationlogs",
            result_field="operation_logs",
            from_date=from_date,
            to_date=to_date,
            cursor=cursor,
        )

    def get_activities(
        self,
        from_date: Optional[str],
        to_date: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of activity logs from Zoom which match the provided filters.

        :param from_date: The required date of the earliest log entry.
        :param to_date: The required date of the latest log entry.
        :param cursor: The cursor to use when paging.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        return self.get_logs(
            endpoint="v2/report/activities",
            result_field="activity_logs",
            from_date=from_date,
            to_date=to_date,
            cursor=cursor,
        )

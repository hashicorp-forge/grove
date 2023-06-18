# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Torq API client."""

import base64
import logging
from typing import Any, Dict, Optional

import requests

from grove.exceptions import RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://api.torq.io/public"
API_PAGE_SIZE = 100


class Client:
    def __init__(
        self,
        base_url: Optional[str] = API_BASE_URI,
        identity: Optional[str] = None,
        key: Optional[str] = None,
        bearer_token: Optional[str] = None,
        retry: Optional[bool] = True,
    ) -> None:
        """Setup a new client.

        :param base_url: The Torq api url - default is https://api.tor.io/public
        :param identity: The Torq integration client id
        :param key: The Torq integration client secret
        :param bearer_token: The allocated bearer token provided by Torq given our
            client id and client secret
        :param retry: Whether to automatically retry if recoverable errors are
            encountered, such as rate-limiting.
        """
        self.base_url = base_url
        self.key = key
        self.identity = identity
        self.bearer_token = bearer_token
        self.retry = retry
        self.logger = logging.getLogger(__name__)
        self._set_default_headers()

        if self.bearer_token is None:
            self.refresh_bearer_token()

    def _set_default_headers(self):
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

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
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
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
        except requests.exceptions.HTTPError as err:
            raise RequestFailedException(err)

        return response.json()

    def refresh_bearer_token(self):
        """Use Basic Auth to get Bearer Token

        :returns: If the request is successful, the bearer token is returned to
            the Client class header.
        """
        auth_url = "https://auth.torq.io/v1/auth/token"

        # set basic auth value for authorisation header
        basic_auth = str(
            base64.b64encode(bytes(f"{self.identity}:{self.key}", "utf-8")),
            "utf-8",
        )

        bearer_response = self._post(
            auth_url,
            data={
                "grant_type": "client_credentials",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "Authorization": f"Basic {basic_auth}",
            },
        )

        self.bearer_token = bearer_response.get("access_token")

        if self.headers is None or self.headers == {}:
            self._set_default_headers()

        self.headers["Authorization"] = f"Bearer {self.bearer_token}"

    def get_logs(
        self,
        endpoint: str,
        result_field: str,
        start_time: Optional[str] = None,
        limit: Optional[int] = API_PAGE_SIZE,
        cursor: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of logs from Torq which match the provided filters.

        :param start_time: The required date and time of the earliest log entry. Start
            times are in RFC3339 format, for example, 2022-03-09T08:40:18.490771179Z.
        :param result_field: The key name for the list of logs in the returned json.
        :param to_date: The required date and time in UTC of the latest log entry.
        :param limit: The maximum number of items to include in a single response.
        :param cursor: The cursor to use when paging.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        result = self._get(
            f"{self.base_url}/{endpoint}",
            params={
                "start_time": start_time,
                "page_size": str(limit),
                "page_token": cursor,
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

    def get_audit_logs(
        self, start_time: Optional[str] = None, cursor: Optional[str] = None
    ) -> AuditLogEntries:
        """Fetches a list of audit logs from Torq which match the provided filters.

        :param start_time: The required date and time of the earliest log entry. Start
            times are in RFC 3339 format, for example, 2022-03-09T08:40:18.490771179Z.
        :param to_date: The required date and time in UTC of the latest log entry.
        :param cursor: The cursor to use when paging.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        return self.get_logs(
            endpoint="v1alpha/audit_logs",
            result_field="audit_logs",
            start_time=start_time,
            cursor=cursor,
        )

    def get_activity_logs(
        self, start_time: Optional[str] = None, cursor: Optional[str] = None
    ) -> AuditLogEntries:
        """Fetches a list of activity logs from Torq which match the provided filters.

        :param start_time: The required date and time of the earliest log entry. Start
            times are in RFC 3339 format, for example, 2022-03-09T08:40:18.490771179Z.
        :param to_date: The required date and time in UTC of the latest log entry.
        :param cursor: The cursor to use when paging.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        return self.get_logs(
            endpoint="v1alpha/activity_logs",
            result_field="activity_logs",
            start_time=start_time,
            cursor=cursor,
        )

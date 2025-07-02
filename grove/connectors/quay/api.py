# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Quay.io Audit API client."""

import logging
import time
from typing import Any, Dict, Optional

import requests

from email.utils import parsedate_to_datetime

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://quay.io/api/v1"
API_ORGANIZATION = "/organization/{identity}/logs" # org name will be inserted from config


class Client:
    def __init__(
        self,
        identity: Optional[str] = None,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new Quay.io API client.

        :param token: Quay.io API Bearer token.
        :param params: Quay.io parameters for the REST API query
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        self.api_base_uri = API_BASE_URI
        self.identity = identity
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}"
        }
        self.retry = retry

        self.logger = logging.getLogger(__name__)

    # private implementation details
    def _get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> HTTPResponse:
        """A GET wrapper to handle retries for the caller.

        param url: URL to perform the HTTP GET against.
        param params: HTTP parameters to add to the request.

        :raises RateLimitException: A rate limit was encountered.
        :raises RequestFailedException: An HTTP request failed.
        
        :return: HTTP Response object containing the headers and body of a response.
        """
        # retry loop
        while True: 
            try: 
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params,
                )
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as err:
                # retry on rate-limit:
                # note: docs do not indicate a rate limit header, or a specific wait-period.
                if getattr(err.response, "status_code", None) == 429:
                    self.logger.warning("Rate-limit was exceeded during request")
                    if self.retry:
                        time.sleep(1) 
                        continue
                    else:
                        raise RateLimitException(err)

                raise RequestFailedException(err)
        return HTTPResponse(headers=response.headers, body=response.json())
    
    # public method wrapper to get organization audit logs
    def get_organization_logs(
        self,
        after: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> AuditLogEntries:
        """Get audit logs for a specific organization.

        :param organization: The name of the organization to retrieve logs for.
        :param params: Additional parameters for the API request.

        :return: AuditLogEntries containing the logs for the organization.
        """

        # convert 'after' string timestamp to just the day because the API only supports
        after_timestamp = None
        start_time = None
        if after:
            try:
                after_timestamp = parsedate_to_datetime(after)
                # querying by day (api limitation -> does not take hour, minute, and/or second)
                start_time = after_timestamp.strftime("%m/%d/%Y")
            except ValueError as e:
                raise ValueError(f"Invalid 'after' timestamp format: {e}")

        # build parameters for the API request
        params = {
            "starttime": start_time,
            "next_page": cursor,
        }
        
        url = f"{self.api_base_uri}{API_ORGANIZATION.format(identity=self.identity)}"
        response = self._get(url, params=params)

        # filter out logs that are before the 'after' timestamp
        filtered = []
        for entry in response.body.get("logs", []):
            date_time = parsedate_to_datetime(entry.get("datetime", ""))
            if after_timestamp and date_time < after_timestamp:
                continue
            filtered.append(entry)
        
        return AuditLogEntries(
            entries=filtered,
            cursor=response.body.get("next_page"),
    )
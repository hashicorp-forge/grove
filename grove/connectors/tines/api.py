# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Tines Audit API client.

This is a bare-bones client designed to interact with audit related APIs only.
"""

import logging
import time
from typing import Dict, Optional

import jmespath
import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://{identity}.{domain}/api/v1"
API_PAGE_SIZE = 500


class Client:
    def __init__(
        self,
        identity: Optional[str] = None,
        domain: str = "tines.com",
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new Tines API client.

        :param token: Tines API token.
        :param identity: The name of the Tines tenant to collect logs from.
        :param domain: The Tines domain to use when constructing API URLs. This is not
            usually required if using Tines hosted tenants.
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        self.retry = retry
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "content-type": "application/json",
            "x-user-token": token,
        }
        self._api_base_uri = API_BASE_URI.format(identity=identity, domain=domain)

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
                response = requests.get(
                    url,
                    headers=self.headers,  # type: ignore
                    params=params,
                )
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

    def list_audit_logs(
        self,
        after: Optional[str] = None,
        operation_name: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit logs which match the provided filters.

        :param after: An RFC3339 timestamp, without milliseconds, to collect logs after.
        :param operation_name: An optional operation to collect logs for.
        :param cursor: Cursor to use when fetching events (pagination). This is the
            page number in the context of Tines.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        # See psf/requests issue #2651 for why we can happily pass in None values and
        # not have the request key added to the URI.
        result = self._get(
            f"{self._api_base_uri}/audit_logs",
            params={
                "page": cursor,
                "after": after,
                "operation_name": operation_name,
                "per_page": str(API_PAGE_SIZE),
            },
        )

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(
            cursor=jmespath.search("meta.next_page", result.body),
            entries=jmespath.search("audit_logs", result.body),
        )

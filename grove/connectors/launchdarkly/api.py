# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""LaunchDarkly Audit API client."""

import logging
import time
from typing import Any, Dict, Optional, List
from jmespath import search

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://app.launchdarkly.com"


class Client:
    def __init__(
        self,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new client.

        :param token: The LaunchDarkly API Key.
        :param retry: Whether to automatically retry if recoverable errors are
            encountered, such as rate-limiting.
        """
        self.retry = retry
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Authorization": f"{token}"
        }

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
                u = url
                v = self.headers
                w = params
                response = requests.get(u, headers=v, params=w)
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

    def get_audit_record_id(
            self,
            id: str
    ) -> HTTPResponse:
        url = f"{API_BASE_URI}/api/v2/auditlog/{id}"
        return self._get(url)

    def get_audit_records_list(
        self,
        cursor: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        limit: Optional[str] = None,
        q: Optional[str] = None,
        spec: Optional[str] = None,
        verbose: bool = False
    ) -> AuditLogEntries:
        """Fetches a list of audit logs which match the provided filters.

        :param cursor: The cursor to use when paging.
        :param before: A timestamp filter, expressed as a Unix epoch time in milliseconds. All entries this returns occurred before the timestamp.
        :param after: A timestamp filter, expressed as a Unix epoch time in milliseconds. All entries this returns occurred after the timestamp.
        :param limit: A limit on the number of audit log entries that return. Set between 1 and 20. The default is 10.
        :param q: Text to search for. You can search for the full or partial name of the resource.
        :param spec: A resource specifier that lets you filter audit log listings by resource

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        # See psf/requests issue #2651 for why we can happily pass in None values
        # and not have the request key added to the URI.

        if cursor is None:
            url = f"{API_BASE_URI}/api/v2/auditlog"
            result = self._get(
                url,
                params={
                    "before": before,
                    "after": after,
                    "limit": limit,
                    "q": q,
                    "spec": spec,
                },
            )
        else:
            url = f"{API_BASE_URI}{cursor}"
            result = self._get(url)

        # Record the cursor, if set.
        cursor = result.body.get("_links", {}).get("next", {}).get("href")

        if(verbose):

            # Pull the list of ids out of the returned results list
            ids = search("items[*]._id", result.body)

            # Iterate through the list of IDs to fetch each detailed audit record
            results: List[Dict[str, Any]] = []
            for id in ids:
                record = self.get_audit_record_id(id)
                results.append(record.body)
            return AuditLogEntries(cursor=cursor, entries=results)

        else:
            return AuditLogEntries(cursor=cursor, entries=result.body.get("items",[]))
            


        # Return the cursor and the results to allow the caller to page as required.


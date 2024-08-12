# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""FleetDM Vulnerability API client.
"""

import logging
import time
from typing import Optional

import jmespath
import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse


class Client:
    def __init__(
        self,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
        params: Optional[dict] = None,
        jmespath_queries: Optional[str] = None,
        api_uri: Optional[str] = None,
    ):
        """Setup a new FleetDM Vulnerability API client.

        :param token: FleetDM API Bearer token.
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        self.retry = retry
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        self.params = params
        self.jmespath_queries = jmespath_queries
        self.api_uri = api_uri

            
    def _get(
        self,
        url: str,
        params: dict,
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

#                p = { "populate_software": "true", "per_page": "100" }
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

    def get_hosts(
        self,
        cursor: Optional[str] = None,
        params: Optional[dict] = None,                                     # get parameters json dict from https://fleetdm.com/docs/rest-api/rest-api#list-hosts
        jmespath_queries: Optional[str] = None,
        api_uri: Optional[str] = None,
    ) -> AuditLogEntries:
        jmespath_queries = jmespath_queries
        
        """Fetches a list of audit logs which match the provided filters.
        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """

        params["page"] = str(cursor)

        # See psf/requests issue #2651 for why we can happily pass in None values and
        # not have the request key added to the URI.
        result = self._get(
            f"{api_uri}/api/v1/fleet/hosts",
            params,
        )

        # The default response for a Fleet Hosts call including software returns a large
        # volume - much more than required. We define a jmespath query string to filter
        # the response from the API down to just the fields we need. The default if none
        # is set is "*" which returns the whole API response
        filteredResults = []
        for host in result.body.get("hosts"):
            filteredResults.append(jmespath.search(jmespath_queries,host))

        # FleetDM returns an empty hosts array if there's no more pages of results,
        # so swap this for None in this case to avoid having to rely on "falsy" conditions.
        # Otherwise, increment the page and continue
        if int(len(result.body.get("hosts"))) == 0:
            cursor = None
        else:
            cursor = str(int(cursor) + 1)

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=filteredResults)

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""FleetDM Vulnerability API client.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

import jmespath
import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse


class Client:
    def __init__(
        self,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
        params: Optional[Dict[str, Any]] = None,
        jmespath_queries: Optional[str] = None,
        api_uri: Optional[str] = None,
    ):
        """Setup a new FleetDM Vulnerability API client.

        :param token: FleetDM API Bearer token.
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        :param params: FleetDM parameters for the REST API query - configurable in the config file
        :param jmespath_queries: A jmespath query string used to filter the responses
        :param api_uri: The base URI to communicate with Fleet
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
        params: Dict[str, Any],
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
        params: Dict[str, Any],
        jmespath_queries: str,
        api_uri: str,
        cursor: Optional[str],
    ) -> AuditLogEntries:
        """Fetches a list of hosts which match the provided filters.
        :param params: get parameters json dict from https://fleetdm.com/docs/rest-api/rest-api#list-hosts
        :param jmespath_queries: jmespath query to filter JSON response as specified at https://jmespath.org/
        :param cursor: The cursor passed from host_logs.py that includes the last update date of systems in Grove
        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """

        # Ensuring that the parameters include the correct After, Order Key, and Order Direction
        # for the pagination and cursor to work correctly
        params["after"] = cursor
        params["order_key"] = "software_updated_at"
        params["order_direction"] = "asc"

        # See psf/requests issue #2651 for why we can happily pass in None values and
        # not have the request key added to the URI.
        result = self._get(
            f"{api_uri}/api/v1/fleet/hosts",
            params,
        )

        filteredResults = []
        # FleetDM returns an empty hosts array if there's no more pages of results,
        # so swap this for a None object
        # Otherwise, grab the last seen update date/time to use in the next page of dates
        if len(result.body.get("hosts", [])) == 0:
            cursor = None
        elif cursor is not None:
            # The default response for a Fleet Hosts call including software returns a large
            # volume - much more than required. We define a jmespath query string to filter
            # the response from the API down to just the fields we need. The default if none
            # is set is "*" which returns the whole API response
            for host in result.body.get("hosts"):
                filteredResults.append(jmespath.search(jmespath_queries,host))
                cursor = host.get("software_updated_at")

        # Return the cursor of the last processed date and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=filteredResults)



    def get_software(
        self,
        params: Dict[str, Any],                                     # get parameters json dict from https://fleetdm.com/docs/rest-api/rest-api#list-hosts
        jmespath_queries: str,
        api_uri: str,
        cursor: Optional[int],
        pointer: datetime
    ) -> AuditLogEntries:
        jmespath_queries = jmespath_queries
        
        """Fetches a list of audit logs which match the provided filters.
        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """

        # Set the page value to the current cursor
        params["page"] = str(cursor)

        # See psf/requests issue #2651 for why we can happily pass in None values and
        # not have the request key added to the URI.
        result = self._get(
            f"{api_uri}/api/v1/fleet/software/versions",
            params,
        )

        filteredResults = []
        # FleetDM returns an empty software array if there's no more pages of results,
        # so swap this for a None object
        # Otherwise, increment the page and continue
        if len(result.body.get("software",[])) == 0:
            cursor = None
        elif cursor is not None:
            cursor = int(cursor) + 1
            # The default response for a Fleet Software call including software returns a large
            # volume - much more than required. We define a jmespath query string to filter
            # the response from the API down to just the fields we need. The default if none
            # is set is "*" which returns the whole API response
            updated_at = result.body.get("counts_updated_at")
            # There's no way in the REST api to ask for only software that has been updated since
            # the last time the software table has last been refreshed. So we manually check
            # the counts_updated_at value against the pointer stored as the last processed time
            # to see if we should log the software again
            if(datetime.fromisoformat(str(updated_at)) > datetime.fromisoformat(str(pointer))):
                for software in result.body.get("software"):
                    s = jmespath.search(jmespath_queries,software)
                    # The software object itself doesn't include an updated at datetime,
                    # so we inherit the counts_updated_at datetime from the base response
                    # and include it in the software object to allow grove to correctly
                    # know the last time the software counts were updated
                    s["updated_at"]=updated_at
                    filteredResults.append(s)


        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=filteredResults)

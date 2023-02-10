# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Marketing Cloud audit API client.

The FuelSDK for Python is quite a heavy client - as it handles both SOAP and REST. The
REST interface is not far off simply using requests either. As a result, to keep things
lightweight this client simply implements the required operations for Get Security
Events and Get Audit Events. There does not appear to be any special handling in this
client - such as for rate-limits or authentication.

This may be replaced in future with the FuelSDK, but for now, we'll keep it simple.
"""

import logging
from typing import Dict, Optional

import requests

from grove.exceptions import RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://{identity}.rest.marketingcloudapis.com"
API_PAGE_SIZE = 500


class Client:
    def __init__(
        self,
        identity: Optional[str] = None,
        token: Optional[str] = None,
    ):
        """Setup a new client.

        Args:
            identity: The SFMC subdomain.
            token: The SFMC API Bearer token.
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
        except requests.exceptions.HTTPError as err:
            raise RequestFailedException(err)

        return HTTPResponse(headers=response.headers, body=response.json()[0])

    def get_audit(
        self,
        kind: str,
        cursor: int = 1,
        startdate: Optional[str] = None,
        enddate: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit events from the relevant audit REST API.

        :param kind: The kind of audit events to fetch.
        :param cursor: The page number of the records to fetch.
        :param latest: The ISO8601 timestamp of the most recent event (inclusive).
        :param oldest: The ISO8601 timestamp of the least recent event (inclusive).

        :return: AuditLogEntries object containing a pagination cursor, and log
            entries.
        """
        # See psf/requests issue #2651 for why we can happily pass in None values
        # and not have the request key added to the URI.
        result = self._get(
            f"{self._api_base_uri}/data/v1/audit/{kind}",
            params={
                "startdate": startdate,
                "enddate": enddate,
                "$pagesize": str(API_PAGE_SIZE),
                "$page": str(cursor),
            },
        )

        # Keep paging until we run out of results.
        if result.body.get("count") == API_PAGE_SIZE:
            cursor += 1
        else:
            cursor = 0

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body.get("items", []))

    def get_audit_events(
        self,
        cursor: int = 1,
        startdate: Optional[str] = None,
        enddate: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit events from the getAuditEvents REST API.

        :param cursor: The page number of the records to fetch.
        :param latest: The ISO8601 timestamp of the most recent event (inclusive).
        :param oldest: The ISO8601 timestamp of the least recent event (inclusive).

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        return self.get_audit(
            kind="auditEvents",
            cursor=cursor,
            startdate=startdate,
            enddate=enddate,
        )

    def get_security_events(
        self,
        cursor: int = 1,
        startdate: Optional[str] = None,
        enddate: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of security events from the getSecurityEvents REST API.

        :param cursor: The page number of the records to fetch.
        :param latest: The ISO8601 timestamp of the most recent event (inclusive).
        :param oldest: The ISO8601 timestamp of the least recent event (inclusive).

        :return: A list of security events.
        """
        return self.get_audit(
            kind="securityEvents",
            cursor=cursor,
            startdate=startdate,
            enddate=enddate,
        )

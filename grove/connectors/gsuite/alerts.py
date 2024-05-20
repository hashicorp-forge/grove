# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Google GSuite Alerts connector for Grove."""

import json
from datetime import datetime, timedelta

import google_auth_httplib2
import httplib2
from google.auth.exceptions import GoogleAuthError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import Error

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RequestFailedException,
)

# This connector is only interested in the GSuite Alerts API.
SCOPES = ["https://www.googleapis.com/auth/apps.alerts"]


class Connector(BaseConnector):
    NAME = "gsuite_alerts"
    POINTER_PATH = "createTime"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        """Collects all alerts from the Google GSuite Alerts API.

        As the Google APIs use OAuth 2.0 2LO ('two-legged OAuth') which contains a
        number of fields inside of a JSON 'service account file' the key and identity
        are treated a little differently in this connector.

        Rather than the key being a single authentication token, the key should contain
        the entire 'service account file' in JSON format - as generated by the Google
        API console.

        The identity must be the name of a service account which has been granted domain
        wide delegation. Please see the following guides for more information:

          https://developers.google.com/admin-sdk/alertcenter/guides/prerequisites
          https://developers.google.com/admin-sdk/directory/v1/guides/delegation

        :raises RequestFailedException: An HTTP request failed.
        """
        cursor = str()
        http = google_auth_httplib2.AuthorizedHttp(
            self.get_credentials(),
            http=self.get_http_transport(),
        )

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago. In the case of the GSuite audit API the pointer is the
        # value of the "createTime" field from the latest record retrieved from the
        # API.
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.utcnow() - timedelta(days=7)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        # Page over all alerts since the last pointer.
        more_requests = True
        page_size = 1000

        with build("alertcenter", "v1beta1", http=http) as service:
            while more_requests:
                # The cursor may work with the filter, but let's not combine them, just
                # to try and avoid any strange and difficult to debug behaviour.
                self.logger.debug(
                    "Requesting GSuite alert list.", extra={"operation": self.operation}
                )
                if cursor:
                    self.logger.debug(
                        "Using pageToken as cursor to request next page of results",
                        extra={"cursor": cursor},
                    )
                    request = service.alerts().list(
                        orderBy="createTime asc",
                        pageSize=page_size,
                        pageToken=cursor,
                    )
                else:
                    self.logger.debug(
                        "Using createTime from pointer",
                        extra={"pointer": self.pointer},
                    )
                    request = service.alerts().list(
                        orderBy="createTime asc",
                        pageSize=page_size,
                        filter=f'createTime > "{self.pointer}"',
                    )

                # Page over results and save.
                try:
                    results = request.execute()
                    alerts = results.get("alerts", [])

                    self.logger.debug(
                        "Got alerts from the GSuite API", extra={"count": len(alerts)}
                    )
                    self.save(alerts)
                except (GoogleAuthError, Error) as err:
                    raise RequestFailedException(f"Request to GSuite API failed: {err}")

                # Determine whether we're still paging.
                if "nextPageToken" not in results:
                    self.logger.debug("No nextPageToken, finishing collection.")
                    more_requests = False
                    break

                self.logger.debug("nextPageToken is present in response, paging.")
                cursor = results["nextPageToken"]
                more_requests = True

    def get_http_transport(self):
        """Creates an HTTP object for use by the Google API Client.

        :return: An httplib2.Http object for use with the Google API client.
        """
        return httplib2.Http()

    def get_credentials(self):
        """Generates and returns a credentials instance from the connector's configured
        service account info. This is used for required to perform operations using the
        Google API client.

        :return: A credentials instance built from configured service account info.

        :raises ConfigurationException: There is an issue with the configuration
            for this connector.
        """
        try:
            service_account_info = json.loads(self.key)
        except json.JSONDecodeError as err:
            raise ConfigurationException(
                f"Unable to load service account JSON for {self.identity}: {err}"
            )

        # Construct the credentials, including scopes and delegation.
        try:
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=SCOPES,
                subject=self.identity,
            )
        except ValueError as err:
            raise ConfigurationException(
                "Unable to generate credentials from service account info for "
                f"{self.identity}: {err}"
            )

        return credentials

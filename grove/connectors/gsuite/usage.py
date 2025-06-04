# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Google GSuite Usage connector for Grove."""

import json
from datetime import datetime, timedelta, timezone

import google_auth_httplib2
import httplib2
from google.auth.exceptions import GoogleAuthError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import Error as GACError

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RequestFailedException,
)

# This connector is only interested in GSuite Usage Reports.
SCOPES = ["https://www.googleapis.com/auth/admin.reports.usage.readonly"]


def as_rfc3339(date: datetime) -> str:
    """Formats an input date into RFC3339 format.

    :param date: The input datetime object to convert to RFC3339 format.

    :return: An RFC 3339 format date as a string.
    """
    return date.isoformat(sep="T", timespec="milliseconds") + "Z"


class Connector(BaseConnector):
    CONNECTOR = "gsuite_usage"
    POINTER_PATH = "date"
    LOG_ORDER = CHRONOLOGICAL


    @property
    def delay(self):
        """Defines the amount of time to delay collection of logs (in minutes).

        This is used to allow time for logs to become 'consistent' before they are
        collected. Google backfill log entries based on their published lag guidelines.
        As a result of this, collection of events within this lag window may result in
        missed events.

        As a result of these constraints, this value is configurable to allow operators
        to preference consistency over speed of delivery, and vice versa. For example,
        a delay of 20 would instruct Grove to only collect logs after they are at least
        20 minutes old.

        This defaults to 0 (no delay).

        :return: The "delay" component of the connector configuration.
        """
        try:
            candidate = self.configuration.delay
        except AttributeError:
            return 0

        try:
            candidate = int(candidate)
        except ValueError as err:
            raise ConfigurationException(
                f"Configured 'delay' is not valid. Value must be an integer. {err}"
            )

        return candidate

    def collect(self):
        """Collects usage reports from the Google GSuite Reports API.

        This method retrieves usage reports for the entire enterprise or users for a specific date range,
        starting from 7 days ago if no pointer is stored, and paginates through the results.

        :raises RequestFailedException: An HTTP request failed.
        """
        self.logger.debug("Beginning data collection from GSuite API.")
        cursor = None
        http = google_auth_httplib2.AuthorizedHttp(
            self.get_credentials(),
            http=self.get_http_transport(),
        )

        # Determine the report type to query.
        try:
            report_type = self.configuration.usage_report_type
            supported_types = ["customerUsageReports", "userUsageReports", "entityUsageReports"]
            if report_type not in supported_types:
                raise ConfigurationException(
                    f"Invalid usage_report_type: {report_type}. Must be one of {supported_types}"
                )
            if report_type == "entityUsageReports" and not getattr(self.configuration, "entity_type", None):
                raise ConfigurationException(
                    "Missing configuration: 'entity_type' must be specified when 'usage_report_type' is 'entityUsageReports'."
                )
        except AttributeError:
            raise ConfigurationException(
                "Missing configuration: 'usage_report_type' must be specified in the connector configuration."
            )

        # If no pointer is stored, set it to 7 days ago.
        # instantiate current datetime in utc
        now = datetime.now(tz=timezone.utc)

        try:
            start_date = datetime.strptime(self.pointer, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except NotFoundException:
            start_date = now - timedelta(days=7)
            self.pointer = start_date.strftime("%Y-%m-%d")

        # Iterate through each day up to today.
        end_date = now
        current_date = start_date

        while current_date <= end_date:
            report_date = current_date.strftime("%Y-%m-%d")
            more_requests = True

            with build("admin", "reports_v1", http=http) as service:
                while more_requests:
                    try:
                        self.logger.debug(
                            "Requesting usage reports for date.",
                            extra={"date": report_date, "cursor": cursor, **self.log_context},
                        )

                        # Build the request based on the report type.
                        if report_type == "customerUsageReports":
                            if cursor:
                                request = service.customerUsageReports().get(
                                    date=report_date, pageToken=cursor
                                )
                            else:
                                request = service.customerUsageReports().get(date=report_date)
                        elif report_type == "userUsageReports":
                            if cursor:
                                request = service.userUsageReport().get(
                                    userKey="all", date=report_date, pageToken=cursor
                                )
                            else:
                                request = service.userUsageReport().get(userKey="all", date=report_date)
                        elif report_type == "entityUsageReports":
                            if cursor:
                                request = service.entityUsageReports().get(
                                    entityType=self.configuration.entity_type, entityKey="all", date=report_date, pageToken=cursor
                                )
                            else:
                                request = service.entityUsageReports().get(
                                     entityType=self.configuration.entity_type, entityKey="all", date=report_date
                                )

                        # Execute the request and process the results.
                        results = request.execute()
                        usage_reports = results.get("usageReports", [])

                        self.logger.debug(
                            "Got usage reports from the GSuite API.",
                            extra={
                                "count": len(usage_reports),
                                "date": report_date,
                                **self.log_context,
                            },
                        )
                        self.save(usage_reports)

                        # Check for pagination.
                        if "nextPageToken" in results:
                            self.logger.debug("Next page token found, continuing to next page.")
                            cursor = results["nextPageToken"]
                            self.logger.debug(
                                "nextPageToken is present, paging.",
                                extra={"cursor": cursor, **self.log_context},
                            )
                        else:
                            self.logger.debug(
                                "No nextPageToken, finishing collection for this date.",
                                extra=self.log_context,
                            )
                            more_requests = False

                    except (GoogleAuthError, GACError) as err:
                        raise RequestFailedException(f"Request to GSuite API failed: {err}")

            # Move to the next day.
            current_date += timedelta(days=1)
            cursor = None  # Reset the cursor for the next day's pagination.


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
                f" {self.identity}: {err}"
            )

        return credentials
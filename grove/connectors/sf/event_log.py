# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Event Log connector for Grove."""

import csv
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import requests
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceError

from grove.connectors.sf.base import (
    SF_TIMESTAMP_FORMAT,
    SF_VERSION,
    BaseSalesforceConnector,
)
from grove.constants import CHRONOLOGICAL, DEFAULT_OPERATION
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RequestFailedException,
)

SF_OPERATIONS = ["Login", DEFAULT_OPERATION]

# SOQL query templates for use when accessing logs.
SOQL_EVENTLOGFILE = (
    "SELECT Id, ApiVersion, EventType, CreatedDate, LogDate, LogFile "
    "FROM EventLogFile "
    "WHERE EventType = '{event}' "
    "AND LogDate >= {pointer}"
)


class Connector(BaseSalesforceConnector):
    CONNECTOR = "sf_event_log"
    POINTER_PATH = "TIMESTAMP_DERIVED"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):  # noqa: C901
        """Collects EventLogs from the SF Cloud API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.

        :raises RequestFailedException: An HTTP request failed.
        :raises ConfigurationException: An issue was found with the configuration for
            this connector.
        """
        # Determine authentication method and get credentials
        if self._is_oauth_configured():
            self.logger.debug("Using OAuth 2.0 client credentials authentication")
            access_token, instance_url = self.get_oauth_access_token()
            session_id = access_token
        elif self._is_legacy_configured():
            self.logger.debug("Using legacy username/password authentication")
            session_id, instance_url = self.get_legacy_credentials()
        else:
            raise ConfigurationException(
                "Either OAuth 2.0 credentials (client_id, client_secret) or legacy "
                "credentials (key, identity, token) must be provided"
            )

        # Create Salesforce client using obtained credentials
        try:
            client = Salesforce(
                instance_url=instance_url,
                session_id=session_id,
                version=SF_VERSION,
            )
        except SalesforceError as err:
            raise RequestFailedException(f"Unable to create Salesforce client: {err}")

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago.
        now = datetime.now(timezone.utc)

        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (now - timedelta(days=7)).strftime(SF_TIMESTAMP_FORMAT)

        # Pointers are stored as strings, so cast to a datetime object for use when
        # constructing filters later on.
        pointer_native = datetime.strptime(self.pointer, SF_TIMESTAMP_FORMAT)

        if self.operation not in SF_OPERATIONS:
            raise ConfigurationException(
                f"Operation must be one of {SF_OPERATIONS}, got '{self.operation}'"
            )

        log_files: Dict[str, List[Any]] = {}
        next_records_url = None

        while True:
            # Page if required. Otherwise, fetch ALL log entries from midnight on the
            # day the last pointer was recorded at. This is required as the SalesForce
            # API appears to split EventLogFiles by date, and does not allow filtering
            # inside of logs using SOQL. See below for more information.
            try:
                if next_records_url is not None:
                    records = client.query_more(next_records_url)  # type: ignore
                else:
                    # Build SOQL query based on operation
                    if self.operation == DEFAULT_OPERATION:
                        # For "all" operation, get all event types
                        soql_query = (
                            "SELECT Id, ApiVersion, EventType, CreatedDate, LogDate, LogFile "
                            "FROM EventLogFile "
                            f"WHERE LogDate >= {pointer_native.strftime('%Y-%m-%dT00:00:00.00Z')}"
                        )
                    else:
                        # For specific operations (like "Login"), filter by EventType
                        soql_query = SOQL_EVENTLOGFILE.format(
                            event=self.operation,
                            pointer=pointer_native.strftime("%Y-%m-%dT00:00:00.00Z"),
                        )

                    records = client.query_all(soql_query)
            except (SalesforceError, requests.exceptions.RequestException) as err:
                raise RequestFailedException(
                    f"Unable to query SalesForce for event logs: {err}"
                )

            # Fetch a list of logs, which will be fetched and processed later - as the
            # SalesForce API returns a REFERENCE to log files here, not the log data
            # itself.
            for record in records.get("records", []):
                record_type = record.get("EventType")
                if record_type not in log_files:
                    log_files[record_type] = []

                # For "all" operation, organize by EventType; for specific operations, use operation name
                log_key = (
                    record_type
                    if self.operation == DEFAULT_OPERATION
                    else self.operation
                )
                if log_key not in log_files:
                    log_files[log_key] = []

                log_files[log_key].append(
                    {
                        "Id": record.get("Id"),
                        "LogFile": record.get("LogFile"),
                        "ApiVersion": record.get("ApiVersion"),
                        "LogDate": record.get("LogDate"),
                        "CreatedDate": record.get("CreatedDate"),
                        "EventType": record_type,
                    }
                )

            # Determine if more requests are needed, otherwise, break out.
            if records.get("nextRecordsUrl") is None:
                break

            next_records_url = records.get("nextRecordsUrl")

        # Manually process out events after the pointer. This is required as the SOQL
        # WHERE filter doesn't appear to allow searching on LogFile contents, only on
        # the LogDate / CreatedDate metadata, which isn't helpful.
        for log_type in log_files:
            for log_file in log_files.get(log_type, {}):
                # Use Requests to get the LogFile directly, passing in the session ID
                # from our authentication session. It doesn't appear that Simple Salesforce
                # has a nice way to handle this for us.
                try:
                    # Ensure the instance_url has a proper scheme
                    if not instance_url.startswith(("http://", "https://")):
                        instance_url = f"https://{instance_url}"

                    request = requests.get(
                        f"{instance_url}/{log_file.get('LogFile')}",
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {session_id}",
                        },
                    )
                except requests.exceptions.RequestException as err:
                    raise RequestFailedException(
                        f"Unable to retrieve event log from SalesForce: {err}"
                    )

                # Convert the CSV (?!) into a nicely JSON serialisable format.
                entries = []

                for entry in csv.DictReader(str(request.content, "utf-8").split("\n")):
                    # Skip if the entry is BEFORE the known pointer - this is required
                    # to handle partial logs from SalesForce. This is expensive, but it
                    # should reduce the need for deduplication later in the pipeline.
                    entry_time = datetime.strptime(
                        entry["TIMESTAMP_DERIVED"], SF_TIMESTAMP_FORMAT
                    )
                    if entry_time.timestamp() <= pointer_native.timestamp():
                        continue

                    entries.append(entry)

                self.save(entries)

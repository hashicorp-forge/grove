# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Event Log connector for Grove."""

import csv
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from simple_salesforce import Salesforce, SalesforceLogin
from simple_salesforce.exceptions import SalesforceError

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RequestFailedException,
)

# TODO: Make this dynamic?
SF_VERSION = "51.0"
SF_OPERATIONS = ["Login"]
SF_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# SOQL query templates for use when accessing logs.
SOQL_EVENTLOGFILE = (
    "SELECT Id, ApiVersion, EventType, CreatedDate, LogDate, LogFile "
    "FROM EventLogFile "
    "WHERE EventType = '{event}' "
    "AND LogDate >= {pointer}"
)


class Connector(BaseConnector):
    CONNECTOR = "sf_event_log"
    POINTER_PATH = "TIMESTAMP_DERIVED"
    LOG_ORDER = CHRONOLOGICAL

    @property
    def token(self):
        """Fetches the SalesForce token from the configuration.

        This is required as this is a third authentication element required by SF.

        :return: The "token" portion of the connector's configuration.
        """
        try:
            return self.configuration.token
        except AttributeError:
            return None

    @property
    def client_id(self) -> Optional[str]:
        """Fetches the OAuth2 client ID from the configuration.

        :return: The "client_id" portion of the connector's configuration.
        """
        try:
            return self.configuration.client_id
        except AttributeError:
            return None

    @property
    def client_secret(self) -> Optional[str]:
        """Fetches the OAuth2 client secret from the configuration.

        :return: The "client_secret" portion of the connector's configuration.
        """
        try:
            return self.configuration.client_secret
        except AttributeError:
            return None

    @property
    def refresh_token(self) -> Optional[str]:
        """Fetches the OAuth2 refresh token from the configuration.

        :return: The "refresh_token" portion of the connector's configuration.
        """
        try:
            return self.configuration.refresh_token
        except AttributeError:
            return None

    @property
    def instance_url(self) -> Optional[str]:
        """Fetches the Salesforce instance URL from the configuration.

        :return: The "instance_url" portion of the connector's configuration.
        """
        try:
            return self.configuration.instance_url
        except AttributeError:
            return None

    @property
    def use_oauth2(self) -> bool:
        """Determines if OAuth2 authentication should be used.

        :return: True if OAuth2 credentials are available, False otherwise.
        """
        return all([
            self.client_id is not None,
            self.client_secret is not None,
            self.refresh_token is not None,
            self.instance_url is not None
        ])

    def _authenticate_oauth2(self) -> Salesforce:
        """Authenticates with Salesforce using OAuth2.

        :raises RequestFailedException: OAuth2 authentication failed.
        :return: Authenticated Salesforce client.
        """
        try:
            client = Salesforce(
                instance_url=self.instance_url,
                session_id=None,
                oauth2={
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'refresh_token': self.refresh_token,
                }
            )
            return client
        except (SalesforceError, requests.exceptions.RequestException) as err:
            raise RequestFailedException(
                f"Unable to authenticate with SalesForce using OAuth2: {err}"
            )

    def _authenticate_username_password(self) -> Salesforce:
        """Authenticates with Salesforce using username/password.

        :raises RequestFailedException: Username/password authentication failed.
        :return: Authenticated Salesforce client.
        """
        session = requests.session()
        try:
            (sf_session, sf_instance) = SalesforceLogin(
                session=session,
                username=self.identity,
                password=self.key,
                security_token=self.token,
            )
            client = Salesforce(
                instance=sf_instance,
                session_id=sf_session,
                version=SF_VERSION,
            )
            return client
        except (SalesforceError, requests.exceptions.RequestException) as err:
            raise RequestFailedException(
                f"Unable to authenticate with SalesForce using username/password: {err}"
            )

    def collect(self):  # noqa: C901
        """Collects EventLogs from the SF Cloud API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.

        :raises RequestFailedException: An HTTP request failed.
        :raises ConfigurationException: An issue was found with the configuration for
            this connector.
        """
        # Authenticate using the appropriate method
        if self.use_oauth2:
            client = self._authenticate_oauth2()
        else:
            client = self._authenticate_username_password()

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
                    records = client.query_all(
                        SOQL_EVENTLOGFILE.format(
                            event=self.operation,
                            pointer=pointer_native.strftime("%Y-%m-%dT00:00:00.00Z"),
                        ),
                    )
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

                log_files[self.operation].append(
                    {
                        "Id": record.get("Id"),
                        "LogFile": record.get("LogFile"),
                        "ApiVersion": record.get("ApiVersion"),
                        "LogDate": record.get("LogDate"),
                        "CreatedDate": record.get("CreatedDate"),
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
                # Use Requests to get the LogFile directly, passing in the session id
                # from our current session. It doesn't appear that Simple Salesforce
                # has a nice way to handle this for us.
                try:
                    # For OAuth2, we need to use the access token from the client
                    if self.use_oauth2:
                        auth_header = f"Bearer {client.access_token}"
                        base_url = self.instance_url
                    else:
                        auth_header = f"Bearer {client.session_id}"
                        base_url = f"https://{client.sf_instance}"

                    request = requests.get(
                        f"{base_url}/{log_file.get('LogFile')}",
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": auth_header,
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

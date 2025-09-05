# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Event Log connector for Grove."""

import csv
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from simple_salesforce import Salesforce, SalesforceLogin
from simple_salesforce.exceptions import SalesforceError

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL, DEFAULT_OPERATION
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RequestFailedException,
)

# TODO: Make this dynamic?
SF_VERSION = "51.0"
SF_OPERATIONS = ["Login", DEFAULT_OPERATION]
SF_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# Default OAuth 2.0 endpoint for Salesforce (fallback only)
SF_OAUTH_TOKEN_URL_DEFAULT = "https://login.salesforce.com/services/oauth2/token"

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
    def client_id(self):
        """Fetches the Salesforce client ID from the configuration.

        This is required for OAuth 2.0 client credentials flow.

        :return: The "client_id" portion of the connector's configuration.
        """
        try:
            return self.configuration.client_id
        except AttributeError:
            return None

    @property
    def client_secret(self):
        """Fetches the Salesforce client secret from the configuration.

        This is required for OAuth 2.0 client credentials flow.

        :return: The "client_secret" portion of the connector's configuration.
        """
        try:
            return self.configuration.client_secret
        except AttributeError:
            return None

    @property
    def instance_url(self):
        """Fetches the Salesforce instance URL from the configuration.

        This is the Salesforce instance URL where the API calls will be made.

        :return: The "instance_url" portion of the connector's configuration.
        """
        try:
            return self.configuration.instance_url
        except AttributeError:
            return None

    @property
    def token(self):
        """Fetches the SalesForce security token from the configuration.

        This is required for traditional username/password authentication.

        :return: The "token" portion of the connector's configuration.
        """
        try:
            return self.configuration.token
        except AttributeError:
            return None

<<<<<<< HEAD
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
=======
    def _is_oauth_configured(self) -> bool:
        """Determines if OAuth 2.0 credentials are configured.

        :return: True if OAuth credentials are present, False otherwise.
>>>>>>> 0aeb5c4 (Add OAuth 2.0 client credentials flow support to Salesforce connector)
        """
        return bool(self.client_id and self.client_secret)

    def _is_legacy_configured(self) -> bool:
        """Determines if legacy username/password credentials are configured.

        :return: True if legacy credentials are present, False otherwise.
        """
        return bool(self.key and self.identity and self.token)

    def _get_oauth_token_url(self) -> str:
        """Determines the OAuth token URL based on the instance URL.

        Uses the instance URL to construct the OAuth endpoint, falling back to
        the default production endpoint if no instance URL is provided.

        :return: The OAuth token URL for the Salesforce environment.
        """
        if not self.instance_url:
            # If no instance URL is provided, use default production endpoint
            return SF_OAUTH_TOKEN_URL_DEFAULT
        
        # Extract the base domain from the instance URL and construct OAuth endpoint
        # e.g., https://klaviyo.my.salesforce.com -> https://klaviyo.my.salesforce.com/services/oauth2/token
        instance_url = self.instance_url.rstrip('/')
        oauth_url = f"{instance_url}/services/oauth2/token"
        
        return oauth_url

    def get_oauth_access_token(self) -> Tuple[str, str]:
        """Obtains an access token using OAuth 2.0 client credentials flow.

        This method authenticates with Salesforce using the client credentials flow
        and returns an access token for API calls.

        :return: A tuple containing (access_token, instance_url)
        :raises ConfigurationException: If required configuration is missing
        :raises RequestFailedException: If authentication fails
        """
        if not self.client_id:
            raise ConfigurationException("client_id is required for OAuth 2.0 authentication")
        if not self.client_secret:
            raise ConfigurationException("client_secret is required for OAuth 2.0 authentication")

        session = requests.session()
        oauth_token_url = self._get_oauth_token_url()
        
        self.logger.debug(
            f"Using OAuth token URL: {oauth_token_url} for instance: {self.instance_url}",
            extra=self.log_context,
        )
        
        self.logger.debug(
            f"OAuth credentials - Client ID: {self.client_id[:8]}... (length: {len(self.client_id) if self.client_id else 0}), "
            f"Client Secret: {'*' * 8}... (length: {len(self.client_secret) if self.client_secret else 0})",
            extra=self.log_context,
        )
        
        try:
            response = session.post(
                oauth_token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            response.raise_for_status()
            
            token_data = response.json()
            access_token = token_data.get("access_token")
            instance_url = token_data.get("instance_url")
            
            if not access_token:
                raise RequestFailedException("No access token received from Salesforce")
            if not instance_url:
                raise RequestFailedException("No instance URL received from Salesforce")
                
            return access_token, instance_url
            
        except requests.exceptions.HTTPError as err:
            # Enhanced error logging for OAuth failures
            try:
                error_response = err.response.json()
                error_details = error_response.get("error_description", error_response.get("error", "Unknown error"))
                self.logger.error(
                    f"OAuth authentication failed: {error_details}",
                    extra={
                        **self.log_context,
                        "oauth_url": oauth_token_url,
                        "client_id": self.client_id[:8] + "..." if self.client_id else None,
                        "response_status": err.response.status_code,
                        "response_body": error_response,
                    }
                )
            except (ValueError, KeyError):
                # If we can't parse the error response, log the raw response
                self.logger.error(
                    f"OAuth authentication failed with unparseable response: {err}",
                    extra={
                        **self.log_context,
                        "oauth_url": oauth_token_url,
                        "client_id": self.client_id[:8] + "..." if self.client_id else None,
                        "response_status": err.response.status_code,
                        "response_text": err.response.text,
                    }
                )
            
            raise RequestFailedException(
                f"Unable to authenticate with Salesforce using OAuth 2.0: {err}"
            )
        except requests.exceptions.RequestException as err:
            raise RequestFailedException(
                f"Unable to authenticate with Salesforce using OAuth 2.0: {err}"
            )

    def get_legacy_credentials(self) -> Tuple[str, str]:
        """Obtains credentials using traditional username/password authentication.

        This method authenticates with Salesforce using the legacy username/password
        flow and returns session credentials for API calls.

        :return: A tuple containing (session_id, instance_url)
        :raises ConfigurationException: If required configuration is missing
        :raises RequestFailedException: If authentication fails
        """
        if not self.key:
            raise ConfigurationException("key (password) is required for legacy authentication")
        if not self.identity:
            raise ConfigurationException("identity (username) is required for legacy authentication")
        if not self.token:
            raise ConfigurationException("token (security token) is required for legacy authentication")

        session = requests.session()
        try:
            (sf_session, sf_instance) = SalesforceLogin(
                session=session,
                username=self.identity,
                password=self.key,
                security_token=self.token,
            )
<<<<<<< HEAD
            client = Salesforce(
                instance=sf_instance,
                session_id=sf_session,
                version=SF_VERSION,
            )
            return client
        except (SalesforceError, requests.exceptions.RequestException) as err:
            raise RequestFailedException(
                f"Unable to authenticate with SalesForce using username/password: {err}"
=======
            return sf_session, sf_instance
        except (SalesforceError, requests.exceptions.RequestException) as err:
            raise RequestFailedException(
                f"Unable to authenticate with Salesforce using legacy authentication: {err}"
            )

    def collect(self):  # noqa: C901
        """Collects EventLogs from the SF Cloud API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.

        Supports both OAuth 2.0 client credentials flow and traditional username/password
        authentication. OAuth 2.0 is preferred if both are configured.

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
            raise RequestFailedException(
                f"Unable to create Salesforce client: {err}"
>>>>>>> 0aeb5c4 (Add OAuth 2.0 client credentials flow support to Salesforce connector)
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
                log_key = record_type if self.operation == DEFAULT_OPERATION else self.operation
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
                    # For OAuth2, we need to use the access token from the client
                    if self.use_oauth2:
                        auth_header = f"Bearer {client.access_token}"
                        base_url = self.instance_url
                    else:
                        auth_header = f"Bearer {client.session_id}"
                        base_url = f"https://{client.sf_instance}"

                    request = requests.get(
<<<<<<< HEAD
                        f"{base_url}/{log_file.get('LogFile')}",
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": auth_header,
=======
                        f"{instance_url}/{log_file.get('LogFile')}",
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {session_id}",
>>>>>>> 0aeb5c4 (Add OAuth 2.0 client credentials flow support to Salesforce connector)
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

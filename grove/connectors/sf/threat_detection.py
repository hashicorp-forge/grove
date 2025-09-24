# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Threat Detection connector for Grove."""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Tuple

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

# Salesforce API version
SF_VERSION = "51.0"
SF_OPERATIONS = [
    "ApiAnomaly",
    "SessionHijacking",
    "CredentialStuffing",
    "ReportAnomaly",
    "BulkApiResult",
    "GuestUserAnomaly",
    "LoginAnomaly",
    "PermissionSet",
]
SF_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# Default OAuth 2.0 endpoint for Salesforce (fallback only)
SF_OAUTH_TOKEN_URL_DEFAULT = "https://login.salesforce.com/services/oauth2/token"


def parse_salesforce_timestamp(timestamp_str: str) -> datetime:
    """Parse Salesforce timestamp, handling both Z and timezone offset formats.

    Salesforce returns timestamps like:
    - 2025-09-16T12:34:56.000Z (with Z)
    - 2025-09-16T12:34:56.000+00:00 (with timezone offset)

    :param timestamp_str: The timestamp string from Salesforce
    :return: Parsed datetime object
    :raises ValueError: If timestamp cannot be parsed
    """
    if timestamp_str.endswith("Z"):
        # Normalize 'Z' to '+00:00' so %z works
        timestamp_str = timestamp_str[:-1] + "+00:00"

    return datetime.strptime(timestamp_str, SF_TIMESTAMP_FORMAT)


# SOQL query template for threat detection event store tables
# Using FIELDS(ALL) to automatically include all available fields for each EventStore
# This avoids hard-coding field names that may vary between Salesforce orgs
def get_soql_query(operation: str, pointer: str) -> str:
    """Generate SOQL query for a specific threat detection operation.

    :param operation: The threat detection operation (e.g., 'ApiAnomaly')
    :param pointer: The timestamp pointer for the WHERE clause
    :return: Formatted SOQL query string
    """
    return (
        f"SELECT FIELDS(ALL) FROM {operation}EventStore "
        f"WHERE EventDate >= {pointer} LIMIT 200"
    )


class Connector(BaseConnector):
    """Collects SalesForce Threat Detection events from Shield Event Monitoring.

    This connector requires:
    - Salesforce Shield license and Event Monitoring enabled
    - User with 'View Event Monitoring Data' permission
    - Proper field-level security for threat detection event stores

    Common issues:
    - INVALID_TYPE errors usually indicate missing Shield licensing
    - INVALID_FIELD errors usually indicate field-level permission issues
    - Contact your Salesforce administrator to enable Shield Event Monitoring
    """

    CONNECTOR = "sf_threat_detection"
    POINTER_PATH = "EventDate"
    LOG_ORDER = CHRONOLOGICAL

    def __init__(self, config: Any, context: Dict[str, Any]) -> None:
        """Initialize the connector with a configuration and context.

        :param config: Configuration options from the connector configuration file.
        :param context: Context about the connector's execution environment.
        """
        super().__init__(config, context)

        # Store configuration attributes that the base class expects
        self.key = getattr(self.configuration, "key", None) or ""
        self.identity = getattr(self.configuration, "identity", None) or ""

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

    @property
    def start_date(self):
        """Fetches the start_date from the configuration.

        This is used for backfill configuration to specify when to start collecting data.

        :return: The "start_date" portion of the connector's configuration.
        """
        try:
            return self.configuration.start_date
        except AttributeError:
            return None

    @property
    def max_retries(self):
        """Fetches the max_retries from the configuration.

        This is used for rate limiting configuration to specify maximum retry attempts.

        :return: The "max_retries" portion of the connector's configuration.
        """
        try:
            return self.configuration.max_retries
        except AttributeError:
            return 3

    @property
    def retry_delay(self):
        """Fetches the retry_delay from the configuration.

        This is used for rate limiting configuration to specify delay between retries.

        :return: The "retry_delay" portion of the connector's configuration.
        """
        try:
            return self.configuration.retry_delay
        except AttributeError:
            return 1

    def _is_oauth_configured(self) -> bool:
        """Determines if OAuth 2.0 credentials are configured.

        :return: True if OAuth credentials are present, False otherwise.
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
        # e.g., https://company.my.salesforce.com -> https://company.my.salesforce.com/services/oauth2/token
        instance_url = self.instance_url.rstrip("/")
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
            raise ConfigurationException(
                "client_id is required for OAuth 2.0 authentication"
            )
        if not self.client_secret:
            raise ConfigurationException(
                "client_secret is required for OAuth 2.0 authentication"
            )

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
            instance_url = token_data.get("instance_url") or self.instance_url

            if not access_token:
                raise RequestFailedException("No access token received from Salesforce")
            if not instance_url:
                raise RequestFailedException(
                    "No instance URL received from Salesforce; set configuration.instance_url"
                )

            return access_token, instance_url

        except requests.exceptions.HTTPError as err:
            # Enhanced error logging for OAuth failures
            try:
                error_response = err.response.json()
                error_details = error_response.get(
                    "error_description", error_response.get("error", "Unknown error")
                )
                self.logger.error(
                    f"OAuth authentication failed: {error_details}",
                    extra={
                        **self.log_context,
                        "oauth_url": oauth_token_url,
                        "client_id": (
                            self.client_id[:8] + "..." if self.client_id else None
                        ),
                        "response_status": err.response.status_code,
                        "response_body": error_response,
                    },
                )
            except (ValueError, KeyError):
                # If we can't parse the error response, log the raw response
                self.logger.error(
                    f"OAuth authentication failed with unparseable response: {err}",
                    extra={
                        **self.log_context,
                        "oauth_url": oauth_token_url,
                        "client_id": (
                            self.client_id[:8] + "..." if self.client_id else None
                        ),
                        "response_status": err.response.status_code,
                        "response_text": err.response.text,
                    },
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
            raise ConfigurationException(
                "key (password) is required for legacy authentication"
            )
        if not self.identity:
            raise ConfigurationException(
                "identity (username) is required for legacy authentication"
            )
        if not self.token:
            raise ConfigurationException(
                "token (security token) is required for legacy authentication"
            )

        session = requests.session()
        try:
            (sf_session, sf_instance) = SalesforceLogin(
                session=session,
                username=self.identity,
                password=self.key,
                security_token=self.token,
            )
            return sf_session, sf_instance
        except (SalesforceError, requests.exceptions.RequestException) as err:
            raise RequestFailedException(
                f"Unable to authenticate with Salesforce using legacy authentication: {err}"
            )

    def _query_with_retry(self, client: Salesforce, soql_query: str) -> Dict[str, Any]:
        """Execute a SOQL query with rate limit handling and retry logic.

        :param client: Salesforce client instance
        :param soql_query: SOQL query to execute
        :return: Query results
        :raises RequestFailedException: If query fails after all retries
        """
        last_exception: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                return client.query_all(soql_query)
            except SalesforceError as err:
                last_exception = err
                error_code = getattr(err, "error_code", None)

                # Check if this is a rate limit error
                if error_code in ["REQUEST_LIMIT_EXCEEDED", "QUERY_TIMEOUT"] or (
                    hasattr(err, "response") and err.response.status_code == 503
                ):

                    if attempt < self.max_retries:
                        # Exponential backoff with jitter
                        delay = self.retry_delay * (2**attempt) + (time.time() % 1)
                        self.logger.warning(
                            f"Rate limit hit, retrying in {delay:.1f}s (attempt {attempt + 1}/{self.max_retries + 1})",
                            extra=self.log_context,
                        )
                        time.sleep(delay)
                        continue
                    else:
                        self.logger.error(
                            f"Rate limit exceeded after {self.max_retries + 1} attempts",
                            extra=self.log_context,
                        )
                        break
                else:
                    # Non-rate-limit error, don't retry - re-raise the original error
                    raise err
            except Exception as err:
                last_exception = err
                break

        raise RequestFailedException(
            f"Unable to query SalesForce after {self.max_retries + 1} attempts: {last_exception}"
        )

    def _check_shield_availability(self, client: Salesforce) -> None:
        """Check if Salesforce Shield Event Monitoring is available and accessible.

        :param client: Salesforce client instance
        :raises RequestFailedException: If Shield is not available or accessible
        """
        try:
            # Try to query a simple field from ApiAnomalyEventStore to check Shield availability
            # This is a lightweight check that will fail if Shield is not enabled
            test_query = "SELECT Id FROM ApiAnomalyEventStore LIMIT 1"
            client.query(test_query)
            self.logger.debug(
                "Salesforce Shield Event Monitoring is available",
                extra=self.log_context,
            )
        except SalesforceError as err:
            error_code = getattr(err, "error_code", None)
            error_message = str(err)

            # Check for INVALID_TYPE error (Shield not available)
            if error_code == "INVALID_TYPE" or "INVALID_TYPE" in error_message:
                raise RequestFailedException(
                    f"Salesforce Shield Event Monitoring is not available or not properly configured.\n\n"
                    f"Error: {err}\n\n"
                    f"This usually indicates one of the following issues:\n"
                    f"1. Salesforce Shield license is not purchased or enabled in your org\n"
                    f"2. Event Monitoring is not activated in Setup > Event Monitoring Settings\n"
                    f"3. Your user lacks 'View Event Monitoring Data' permission\n"
                    f"4. Your org doesn't have the necessary Shield licenses\n"
                    f"5. The ApiAnomalyEventStore event type is not enabled for data storage\n\n"
                    f"To resolve this:\n"
                    f"- Contact your Salesforce administrator to verify Shield licensing\n"
                    f"- Enable Event Monitoring in Setup > Event Monitoring Settings\n"
                    f"- Ensure your user has the required permissions\n"
                    f"- Verify that EventStore data collection is enabled for the desired event types\n\n"
                    f"Note: Salesforce Shield is required for Threat Detection Event Monitoring features."
                )
            else:
                # Re-raise other errors as they might be different issues
                raise RequestFailedException(
                    f"Salesforce API error during Shield availability check: {err}"
                )

    def collect(self):  # noqa: C901
        """Collects Threat Detection events from the SF Shield Event Monitoring API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.

        :raises RequestFailedException: An HTTP request failed.
        :raises ConfigurationException: An issue was found with the configuration for
            this connector.
        """
        # Validate operation before attempting authentication
        if self.operation not in SF_OPERATIONS:
            raise ConfigurationException(
                f"Operation must be one of {SF_OPERATIONS}, got '{self.operation}'. "
                f"Please specify a valid operation in your configuration file."
            )

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

        # Check if Salesforce Shield Event Monitoring is available
        self._check_shield_availability(client)

        # Determine the starting point for data collection
        now = datetime.now(timezone.utc)

        try:
            _ = self.pointer
        except NotFoundException:
            # No pointer stored, determine start date
            if self.start_date:
                # Use configured start_date for backfill
                try:
                    start_dt = parse_salesforce_timestamp(self.start_date)
                    self.pointer = start_dt.strftime(SF_TIMESTAMP_FORMAT)
                    self.logger.info(
                        f"Using configured start_date for backfill: {self.start_date}",
                        extra=self.log_context,
                    )
                except ValueError:
                    self.logger.warning(
                        f"Invalid start_date format '{self.start_date}', falling back to 7 days ago",
                        extra=self.log_context,
                    )
                    self.pointer = (now - timedelta(days=7)).strftime(
                        SF_TIMESTAMP_FORMAT
                    )
            else:
                # Default to a week ago
                self.pointer = (now - timedelta(days=7)).strftime(SF_TIMESTAMP_FORMAT)

        # Pointers are stored as strings, so cast to a datetime object for use when
        # constructing filters later on.
        pointer_native = parse_salesforce_timestamp(self.pointer)

        # Query for threat detection events using direct event store tables
        try:
            # Use the appropriate event store table for the specified operation
            soql_query = get_soql_query(
                self.operation, pointer_native.strftime("%Y-%m-%dT00:00:00.00Z")
            )

            self.logger.debug(
                f"Using SOQL query: {soql_query}",
                extra=self.log_context,
            )

            records = self._query_with_retry(client, soql_query)

            self.logger.info(
                f"Query returned {records.get('totalSize', 0)} threat detection records",
                extra=self.log_context,
            )

            # Try a broader time range to see if there are any records at all
            if records.get("totalSize", 0) == 0:
                self.logger.info(
                    "No records found in last 7 days. Trying last 30 days...",
                    extra=self.log_context,
                )

                # Rebuild the query with a new pointer timestamp
                broader_pointer = (now - timedelta(days=30)).strftime(
                    "%Y-%m-%dT00:00:00.00Z"
                )
                broader_query = get_soql_query(self.operation, broader_pointer)

                try:
                    broader_records = self._query_with_retry(client, broader_query)
                    self.logger.info(
                        f"Broader query (30 days) returned {broader_records.get('totalSize', 0)} records",
                        extra=self.log_context,
                    )

                    # Use the broader results if we found records
                    if broader_records.get("totalSize", 0) > 0:
                        records = broader_records
                        self.logger.info(
                            "Using broader query results for processing",
                            extra=self.log_context,
                        )
                except Exception as e:
                    self.logger.warning(
                        f"Broader query failed: {e}",
                        extra=self.log_context,
                    )

        except RequestFailedException:
            # Re-raise our custom exceptions
            raise
        except SalesforceError as err:
            # Handle specific Salesforce errors with helpful guidance
            error_code = getattr(err, "error_code", None)
            error_message = str(err)

            if error_code == "INVALID_TYPE" or "INVALID_TYPE" in error_message:
                # This is typically a licensing/permissions issue
                raise RequestFailedException(
                    f"Salesforce Shield licensing or permissions issue detected for '{self.operation}' events.\n\n"
                    f"Error: {error_message}\n\n"
                    f"This usually indicates one of the following issues:\n"
                    f"1. Salesforce Shield license is not purchased or enabled in your org\n"
                    f"2. Event Monitoring is not activated in Setup > Event Monitoring Settings\n"
                    f"3. The '{self.operation}EventStore' event type is not enabled for data storage\n"
                    f"4. Your user lacks 'View Threat Detection Events' permission\n"
                    f"5. Your org doesn't have the necessary Shield licenses for this event type\n\n"
                    f"To resolve this:\n"
                    f"- Contact your Salesforce administrator to verify Shield licensing\n"
                    f"- Enable Event Monitoring in Setup > Event Monitoring Settings\n"
                    f"- Ensure the '{self.operation}' event type is enabled for data storage\n"
                    f"- Verify your user has the required permissions\n\n"
                    f"Note: Some EventStore types may require additional Shield features or licenses."
                )
            elif error_code == "INVALID_FIELD" or "INVALID_FIELD" in error_message:
                # Field-level permissions issue
                raise RequestFailedException(
                    f"Field-level permissions issue detected for '{self.operation}' events.\n\n"
                    f"Error: {error_message}\n\n"
                    f"This usually indicates:\n"
                    f"1. Your user lacks field-level access to the requested fields\n"
                    f"2. The event store schema is different in your Salesforce version\n"
                    f"3. Shield Event Monitoring is not properly configured\n\n"
                    f"To resolve this:\n"
                    f"- Contact your Salesforce administrator to check field permissions\n"
                    f"- Verify Shield Event Monitoring is properly configured\n"
                    f"- Ensure your user has access to all EventStore fields"
                )
            else:
                # Other Salesforce errors
                raise RequestFailedException(
                    f"Salesforce API error for '{self.operation}' events ({error_code}): {error_message}"
                )
        except Exception as err:
            raise RequestFailedException(
                f"Unable to query SalesForce for threat detection events: {err}"
            )

        # Process the threat detection records directly
        entries = []
        self.logger.debug(
            f"Processing {len(records.get('records', []))} records from query",
            extra=self.log_context,
        )

        for i, record in enumerate(records.get("records", [])):
            self.logger.debug(
                f"Processing record {i+1}: EventDate={record.get('EventDate')}, Score={record.get('Score')}",
                extra=self.log_context,
            )

            # Start with basic metadata
            entry = {
                "_grove_operation": self.operation,
                "_grove_connector": self.name,
            }

            # Add the threat detection event fields (common fields)
            entry["Id"] = record.get("Id")
            entry["Score"] = record.get("Score")
            entry["UserId"] = record.get("UserId")
            entry["EventDate"] = record.get("EventDate")
            entry["Summary"] = record.get("Summary")

            # Add all fields from the record dynamically
            # Since we're using FIELDS(ALL), we include all available fields
            for field_name, field_value in record.items():
                if field_name not in ["Id", "Score", "UserId", "EventDate", "Summary"]:
                    # Add any additional fields that aren't already included
                    entry[field_name] = field_value

            # Only include entries that are after the pointer timestamp
            if record.get("EventDate"):
                try:
                    entry_time = parse_salesforce_timestamp(record["EventDate"])
                    if entry_time.timestamp() > pointer_native.timestamp():
                        entries.append(entry)
                        self.logger.debug(
                            f"Record {i+1} added to entries (after pointer)",
                            extra=self.log_context,
                        )
                    else:
                        self.logger.debug(
                            f"Record {i+1} skipped (before or equal to pointer)",
                            extra=self.log_context,
                        )
                except ValueError:
                    # If we can't parse the timestamp, include the entry anyway
                    self.logger.warning(
                        f"Could not parse EventDate '{record.get('EventDate')}' for record {i+1}. Including anyway.",
                        extra=self.log_context,
                    )
                    entries.append(entry)
            else:
                self.logger.debug(
                    f"Record {i+1} has no EventDate, including anyway",
                    extra=self.log_context,
                )
                entries.append(entry)

        # Save the collected entries
        if entries:
            self.logger.info(
                f"Saving {len(entries)} threat detection entries",
                extra=self.log_context,
            )
            self.save(entries)

            # Update pointer to the latest event (EventDate + Id for tie-breaking)
            if entries:
                # Find the latest event by EventDate, then by Id for tie-breaking
                latest_event = None
                latest_time = None
                latest_id = None

                for entry in entries:
                    if entry.get("EventDate") and entry.get("Id"):
                        try:
                            entry_time = parse_salesforce_timestamp(entry["EventDate"])
                            entry_id = entry["Id"]

                            # Compare by EventDate first, then by Id for tie-breaking
                            if (
                                latest_time is None
                                or entry_time > latest_time
                                or (entry_time == latest_time and latest_id is not None and entry_id > latest_id)
                            ):
                                latest_time = entry_time
                                latest_id = entry_id
                                latest_event = entry
                        except ValueError:
                            continue

                if latest_event and latest_event.get("EventDate"):
                    self.pointer = latest_event["EventDate"]
                    self.logger.debug(
                        f"Updated pointer to {self.pointer} (Id: {latest_event.get('Id')})",
                        extra=self.log_context,
                    )
        else:
            self.logger.info(
                "No entries to save after processing records",
                extra=self.log_context,
            )

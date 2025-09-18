# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Setup Audit Trail connector for Grove."""

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
SF_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# Default OAuth 2.0 endpoint for Salesforce (fallback only)
SF_OAUTH_TOKEN_URL_DEFAULT = "https://login.salesforce.com/services/oauth2/token"

# SOQL query template for SetupAuditTrail
SOQL_SETUP_AUDIT_TRAIL = (
    "SELECT Id, Action, Section, CreatedBy.Username, CreatedDate, DelegateUser, Display "
    "FROM SetupAuditTrail "
    "WHERE CreatedDate >= {pointer} "
    "ORDER BY CreatedDate DESC"
)


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


class Connector(BaseConnector):
    """Collects SalesForce Setup Audit Trail events.

    This connector retrieves setup audit trail records that track administrative
    changes made to Salesforce org configuration, user management, and system settings.

    Setup Audit Trail captures:
    - User management changes (create, modify, deactivate users)
    - Permission set and profile modifications
    - Custom object and field creation/modification
    - Security settings changes
    - Integration and API changes
    - Workflow and automation changes

    This connector requires:
    - User with 'View Setup and Configuration' permission
    - Access to SetupAuditTrail object (available in all Salesforce editions)

    Common issues:
    - INVALID_TYPE errors usually indicate missing permissions
    - Contact your Salesforce administrator to verify access to SetupAuditTrail
    """

    CONNECTOR = "sf_setup_audit_trail"
    POINTER_PATH = "CreatedDate"
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

        # Backfill configuration
        self.start_date = getattr(self.configuration, "start_date", None)

        # Rate limiting configuration
        self.max_retries = getattr(self.configuration, "max_retries", 3)
        self.retry_delay = getattr(self.configuration, "retry_delay", 1)

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

    def _is_oauth_configured(self) -> bool:
        """Determines if OAuth 2.0 credentials are configured.

        :return: True if OAuth credentials are present, False otherwise.
        """
        return bool(self.client_id and self.client_secret and not (self.key and self.identity and self.token))

    def _is_legacy_configured(self) -> bool:
        """Determines if legacy username/password credentials are configured.

        :return: True if legacy credentials are present, False otherwise.
        """
        return bool(self.key and self.identity and self.token and not (self.client_id and self.client_secret))

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

    def collect(self):  # noqa: C901
        """Collects Setup Audit Trail events from the SF API.

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

        # Query for setup audit trail events
        try:
            soql_query = SOQL_SETUP_AUDIT_TRAIL.format(
                pointer=pointer_native.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            )

            self.logger.debug(
                f"Using SOQL query: {soql_query}",
                extra=self.log_context,
            )

            records = self._query_with_retry(client, soql_query)

            self.logger.info(
                f"Query returned {records.get('totalSize', 0)} setup audit trail records",
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
                # This is typically a permissions issue
                raise RequestFailedException(
                    f"Salesforce permissions issue detected for SetupAuditTrail.\n\n"
                    f"Error: {error_message}\n\n"
                    f"This usually indicates one of the following issues:\n"
                    f"1. Your user lacks 'View Setup and Configuration' permission\n"
                    f"2. Your user lacks access to the SetupAuditTrail object\n"
                    f"3. Setup Audit Trail is not enabled in your org\n\n"
                    f"To resolve this:\n"
                    f"- Contact your Salesforce administrator to verify permissions\n"
                    f"- Ensure Setup Audit Trail is enabled in Setup > Audit Trail\n"
                    f"- Verify your user has the required permissions\n\n"
                    f"Note: SetupAuditTrail is available in all Salesforce editions."
                )
            else:
                # Other Salesforce errors
                raise RequestFailedException(
                    f"Salesforce API error for SetupAuditTrail ({error_code}): {error_message}"
                )
        except Exception as err:
            raise RequestFailedException(
                f"Unable to query SalesForce for setup audit trail events: {err}"
            )

        # Process the setup audit trail records
        entries = []
        self.logger.debug(
            f"Processing {len(records.get('records', []))} records from query",
            extra=self.log_context,
        )

        for i, record in enumerate(records.get("records", [])):
            self.logger.debug(
                f"Processing record {i+1}: CreatedDate={record.get('CreatedDate')}, Action={record.get('Action')}",
                extra=self.log_context,
            )

            # Start with basic metadata
            entry = {
                "_grove_operation": "SetupAuditTrail",
                "_grove_connector": self.name,
            }

            # Add the setup audit trail event fields
            entry["Id"] = record.get("Id")
            entry["Action"] = record.get("Action")
            entry["Section"] = record.get("Section")
            entry["CreatedDate"] = record.get("CreatedDate")
            entry["Display"] = record.get("Display")

            # Handle CreatedBy.Username (nested object)
            if record.get("CreatedBy"):
                entry["CreatedByUsername"] = record["CreatedBy"].get("Username")
            else:
                entry["CreatedByUsername"] = None

            # Handle DelegateUser (may be null)
            entry["DelegateUser"] = record.get("DelegateUser")

            # Only include entries that are after the pointer timestamp
            if record.get("CreatedDate"):
                try:
                    entry_time = parse_salesforce_timestamp(record["CreatedDate"])
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
                        f"Could not parse CreatedDate '{record.get('CreatedDate')}' for record {i+1}. Including anyway.",
                        extra=self.log_context,
                    )
                    entries.append(entry)
            else:
                self.logger.debug(
                    f"Record {i+1} has no CreatedDate, including anyway",
                    extra=self.log_context,
                )
                entries.append(entry)

        # Save the collected entries
        if entries:
            self.logger.info(
                f"Saving {len(entries)} setup audit trail entries",
                extra=self.log_context,
            )
            self.save(entries)

            # Update pointer to the latest event (CreatedDate + Id for tie-breaking)
            if entries:
                # Find the latest event by CreatedDate, then by Id for tie-breaking
                latest_event = None
                latest_time = None
                latest_id = None

                for entry in entries:
                    if entry.get("CreatedDate") and entry.get("Id"):
                        try:
                            entry_time = parse_salesforce_timestamp(entry["CreatedDate"])
                            entry_id = entry["Id"]

                            # Compare by CreatedDate first, then by Id for tie-breaking
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

                if latest_event and latest_event.get("CreatedDate"):
                    self.pointer = latest_event["CreatedDate"]
                    self.logger.debug(
                        f"Updated pointer to {self.pointer} (Id: {latest_event.get('Id')})",
                        extra=self.log_context,
                    )
        else:
            self.logger.info(
                "No entries to save after processing records",
                extra=self.log_context,
            )

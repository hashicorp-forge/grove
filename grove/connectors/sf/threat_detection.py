# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Threat Detection connector for Grove."""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceError

from grove.connectors.sf.base import (
    SF_TIMESTAMP_FORMAT,
    SF_VERSION,
    BaseSalesforceConnector,
    parse_salesforce_timestamp,
)
from grove.constants import CHRONOLOGICAL
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RequestFailedException,
)

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


class Connector(BaseSalesforceConnector):
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

            entry = dict(record)

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
        else:
            self.logger.info(
                "No entries to save after processing records",
                extra=self.log_context,
            )

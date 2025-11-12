# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Setup Audit Trail connector for Grove."""

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

# SOQL query template for SetupAuditTrail
SOQL_SETUP_AUDIT_TRAIL = (
    "SELECT Id, Action, Section, CreatedBy.Username, CreatedDate, DelegateUser, Display "
    "FROM SetupAuditTrail "
    "WHERE CreatedDate >= {pointer} "
    "ORDER BY CreatedDate DESC"
)


class Connector(BaseSalesforceConnector):
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

        # Backfill configuration
        self.start_date = getattr(self.configuration, "start_date", None)


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
            entry["CreatedByUsername"] = record.get("CreatedBy", {}).get("Username")

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
            # Reverse entries since query returns DESC order, but Grove expects chronological order
            entries.reverse()
            self.save(entries)
        else:
            self.logger.info(
                "No entries to save after processing records",
                extra=self.log_context,
            )

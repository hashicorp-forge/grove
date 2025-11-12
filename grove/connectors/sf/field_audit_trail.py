# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""SalesForce Field Audit Trail connector for Grove."""

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

# SOQL query template for FieldHistoryArchive Big Object
# Must respect Big Object query rules: WHERE clause must use composite key fields
# in order: FieldHistoryType → ParentId or CreatedDate
SOQL_FIELD_AUDIT_TRAIL = (
    "SELECT ParentId, FieldHistoryType, Field, Id, OldValue, NewValue, CreatedDate "
    "FROM FieldHistoryArchive "
    "WHERE FieldHistoryType = '{object_type}' "
    "AND CreatedDate >= {pointer} "
    "LIMIT 1000"
)

# Supported object types for field audit trail
SUPPORTED_OBJECT_TYPES = [
    "Account", "Contact", "Lead", "Opportunity", "Case", "User", "Campaign",
    "Product2", "Pricebook2", "PricebookEntry", "Quote", "Contract", "Asset",
    "Task", "Event", "Note", "Attachment", "Document", "ContentDocument",
    "ContentVersion", "FeedItem", "FeedComment", "CustomObject__c"  # Add custom objects as needed
]


class Connector(BaseSalesforceConnector):
    """Collects SalesForce Field Audit Trail events from FieldHistoryArchive Big Object.

    This connector retrieves field audit trail records that track changes to
    individual field values on Salesforce records. Field Audit Trail (FAT) provides
    detailed history of who changed what field, when, and what the old/new values were.

    Field Audit Trail captures:
    - Field value changes on standard and custom objects
    - User information for who made the change
    - Timestamp of when the change occurred
    - Old and new field values
    - Parent record information

    This connector requires:
    - Salesforce Shield Field Audit Trail license
    - User with 'View Field Audit Trail' permission
    - Field Audit Trail enabled for the specified object types
    - Access to FieldHistoryArchive Big Object

    Common issues:
    - INVALID_TYPE errors usually indicate missing Shield licensing
    - INVALID_FIELD errors usually indicate field-level permission issues
    - Contact your Salesforce administrator to verify Shield Field Audit Trail licensing

    Big Object Query Rules:
    - WHERE clause must use composite key fields in order: FieldHistoryType → ParentId or CreatedDate
    - Limited operators allowed (=, >, <, >=, <=)
    - Must specify FieldHistoryType (object type) in WHERE clause
    """

    CONNECTOR = "sf_field_audit_trail"
    POINTER_PATH = "CreatedDate"
    LOG_ORDER = CHRONOLOGICAL

    def __init__(self, config: Any, context: Dict[str, Any]) -> None:
        """Initialize the connector with a configuration and context.

        :param config: Configuration options from the connector configuration file.
        :param context: Context about the connector's execution environment.
        """
        super().__init__(config, context)

        # Field Audit Trail specific configuration
        self.object_type = getattr(self.configuration, "object_type", None)
        self.start_date = getattr(self.configuration, "start_date", None)

        # Validate object_type is provided and supported
        if not self.object_type:
            raise ConfigurationException(
                "object_type is required for field audit trail connector. "
                f"Supported types: {', '.join(SUPPORTED_OBJECT_TYPES)}"
            )

        if self.object_type not in SUPPORTED_OBJECT_TYPES:
            self.logger.warning(
                f"Object type '{self.object_type}' is not in the supported list. "
                f"Supported types: {', '.join(SUPPORTED_OBJECT_TYPES)}. "
                "Proceeding anyway as it may be a valid custom object.",
                extra=self.log_context,
            )


    def _check_field_audit_trail_availability(self, client: Salesforce) -> None:
        """Check if Salesforce Field Audit Trail is available and accessible.

        :param client: Salesforce client instance
        :raises RequestFailedException: If Field Audit Trail is not available or accessible
        """
        try:
            # Try to query a simple field from FieldHistoryArchive to check availability
            # This is a lightweight check that will fail if Field Audit Trail is not enabled
            test_query = f"SELECT Id FROM FieldHistoryArchive WHERE FieldHistoryType = '{self.object_type}' LIMIT 1"
            client.query(test_query)
            self.logger.debug(
                f"Salesforce Field Audit Trail is available for {self.object_type}",
                extra=self.log_context,
            )
        except SalesforceError as err:
            error_code = getattr(err, "error_code", None)
            error_message = str(err)

            # Check for INVALID_TYPE error (Field Audit Trail not available)
            if error_code == "INVALID_TYPE" or "INVALID_TYPE" in error_message:
                raise RequestFailedException(
                    f"Salesforce Field Audit Trail is not available or not properly configured.\n\n"
                    f"Error: {err}\n\n"
                    f"This usually indicates one of the following issues:\n"
                    f"1. Salesforce Shield Field Audit Trail license is not purchased or enabled\n"
                    f"2. Field Audit Trail is not activated in Setup > Field Audit Trail Settings\n"
                    f"3. Your user lacks 'View Field Audit Trail' permission\n"
                    f"4. Field Audit Trail is not enabled for the '{self.object_type}' object type\n"
                    f"5. Your org doesn't have the necessary Shield licenses\n\n"
                    f"To resolve this:\n"
                    f"- Contact your Salesforce administrator to verify Shield Field Audit Trail licensing\n"
                    f"- Enable Field Audit Trail in Setup > Field Audit Trail Settings\n"
                    f"- Ensure Field Audit Trail is enabled for '{self.object_type}'\n"
                    f"- Verify your user has the required permissions\n\n"
                    f"Note: Salesforce Shield Field Audit Trail license is required for this feature."
                )
            else:
                # Re-raise other errors as they might be different issues
                raise RequestFailedException(
                    f"Salesforce API error during Field Audit Trail availability check: {err}"
                )

    def collect(self):  # noqa: C901
        """Collects Field Audit Trail events from the SF FieldHistoryArchive Big Object.

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

        # Check if Salesforce Field Audit Trail is available
        self._check_field_audit_trail_availability(client)

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

        # Query for field audit trail events using FieldHistoryArchive Big Object
        try:
            soql_query = SOQL_FIELD_AUDIT_TRAIL.format(
                object_type=self.object_type,
                pointer=pointer_native.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            )

            self.logger.debug(
                f"Using SOQL query: {soql_query}",
                extra=self.log_context,
            )

            records = self._query_with_retry(client, soql_query)

            self.logger.info(
                f"Query returned {records.get('totalSize', 0)} field audit trail records for {self.object_type}",
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
                    f"Salesforce Shield Field Audit Trail licensing or permissions issue detected for '{self.object_type}'.\n\n"
                    f"Error: {error_message}\n\n"
                    f"This usually indicates one of the following issues:\n"
                    f"1. Salesforce Shield Field Audit Trail license is not purchased or enabled\n"
                    f"2. Field Audit Trail is not activated in Setup > Field Audit Trail Settings\n"
                    f"3. Field Audit Trail is not enabled for the '{self.object_type}' object type\n"
                    f"4. Your user lacks 'View Field Audit Trail' permission\n"
                    f"5. Your org doesn't have the necessary Shield licenses\n\n"
                    f"To resolve this:\n"
                    f"- Contact your Salesforce administrator to verify Shield Field Audit Trail licensing\n"
                    f"- Enable Field Audit Trail in Setup > Field Audit Trail Settings\n"
                    f"- Ensure Field Audit Trail is enabled for '{self.object_type}'\n"
                    f"- Verify your user has the required permissions\n\n"
                    f"Note: Salesforce Shield Field Audit Trail license is required for this feature."
                )
            elif error_code == "INVALID_FIELD" or "INVALID_FIELD" in error_message:
                # Field-level permissions issue
                raise RequestFailedException(
                    f"Field-level permissions issue detected for FieldHistoryArchive.\n\n"
                    f"Error: {error_message}\n\n"
                    f"This usually indicates:\n"
                    f"1. Your user lacks field-level access to the requested fields\n"
                    f"2. The FieldHistoryArchive schema is different in your Salesforce version\n"
                    f"3. Field Audit Trail is not properly configured\n\n"
                    f"To resolve this:\n"
                    f"- Contact your Salesforce administrator to check field permissions\n"
                    f"- Verify Field Audit Trail is properly configured\n"
                    f"- Ensure your user has access to all FieldHistoryArchive fields"
                )
            else:
                # Other Salesforce errors
                raise RequestFailedException(
                    f"Salesforce API error for FieldHistoryArchive ({error_code}): {error_message}"
                )
        except Exception as err:
            raise RequestFailedException(
                f"Unable to query SalesForce for field audit trail events: {err}"
            )

        # Process the field audit trail records
        entries = []
        self.logger.debug(
            f"Processing {len(records.get('records', []))} records from query",
            extra=self.log_context,
        )

        for i, record in enumerate(records.get("records", [])):
            self.logger.debug(
                f"Processing record {i+1}: CreatedDate={record.get('CreatedDate')}, Field={record.get('Field')}",
                extra=self.log_context,
            )

            # Start with basic metadata
            entry = {
                "_grove_operation": "FieldAuditTrail",
                "_grove_connector": self.name,
                "object_type": self.object_type,
            }

            # Add the field audit trail event fields
            entry["Id"] = record.get("Id")
            entry["ParentId"] = record.get("ParentId")
            entry["FieldHistoryType"] = record.get("FieldHistoryType")
            entry["Field"] = record.get("Field")
            entry["OldValue"] = record.get("OldValue")
            entry["NewValue"] = record.get("NewValue")
            entry["CreatedDate"] = record.get("CreatedDate")

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
                f"Saving {len(entries)} field audit trail entries for {self.object_type}",
                extra=self.log_context,
            )
            self.save(entries)
        else:
            self.logger.info(
                f"No entries to save after processing records for {self.object_type}",
                extra=self.log_context,
            )

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Snowflake query history connector for Grove."""

from datetime import datetime, timedelta, timezone

import snowflake.connector

from grove.connectors.snowflake.common import SnowflakeConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import AccessException, NotFoundException, RequestFailedException

# Define the paramaterised Snowflake query to use to fetch query history records.
SNOWFLAKE_QUERY_QUERY_HISTORY = """
  SELECT QUERY_TEXT,
         USER_NAME,
         ROLE_NAME,
         START_TIME,
         END_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
   WHERE START_TIME > %(pointer)s
ORDER BY START_TIME ASC;
 """


class Connector(SnowflakeConnector):
    NAME = "snowflake_query_history"
    POINTER_PATH = "START_TIME"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        """Collects query history records from Snowflake."""

        # If no pointer is stored then start from 24-hours ago - due to volume.
        try:
            _ = self.pointer
        except NotFoundException:
            self.pointer = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        # Decode the private key from the loaded PEM format PKCS#8 data.
        private_key = self._load_private_key()

        # Connect to Snowflake using configured parameters. We force the timezone to
        # UTC here, to ensure consistency between log sources.
        try:
            client = snowflake.connector.connect(
                role=self.role,
                user=self.identity,
                account=self.account,
                warehouse=self.warehouse,
                private_key=private_key,
                timezone="UTC",
            )
        except snowflake.connector.errors.Error as err:
            raise AccessException(f"Unable to connect to Snowflake. {err}")

        # Fetch the data, and write out the records in batches.
        cursor = client.cursor(snowflake.connector.DictCursor)
        try:
            cursor.execute(SNOWFLAKE_QUERY_QUERY_HISTORY, {"pointer": self.pointer})
        except snowflake.connector.errors.ProgrammingError as err:
            raise RequestFailedException(f"Failed to execute Snowflake query. {err}")

        records = []
        for row in cursor:
            records.append(row)

            if len(records) >= self.batch_size:
                self.save(records)
                records = []

        # Call save one last time to handle the last set of records.
        self.save(records)

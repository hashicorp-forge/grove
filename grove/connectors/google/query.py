# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Google BigQuery connector for Grove."""

import json
from datetime import datetime, timedelta, timezone

from google.auth.exceptions import GoogleAuthError
from google.oauth2 import service_account
from google.cloud import bigquery

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RequestFailedException,
)

def as_bigquery_timestamp(epoch_ms_str: str) -> str:
    """
    Converts a string containing epoch time in milliseconds to a BigQuery-compatible timestamp string.

    :param epoch_ms_str: The epoch time in milliseconds as a string.
    :return: A BigQuery TIMESTAMP formatted date string (YYYY-MM-DD HH:MM:SS+00).
    """
    dt = datetime.fromtimestamp(int(epoch_ms_str) / 1000.0, tz=timezone.utc)
    # BigQuery expects "+00" not "+0000" or "+00:00"
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


class Connector(BaseConnector):
    CONNECTOR = "google_bigquery_query"
    LOG_ORDER = CHRONOLOGICAL

    def collect(self):
        """Collects logs from the specified table using Google BigQuery API."""
        self.logger.info("Starting data collection from BigQuery.")

        try:
            project_id = self.configuration.project_id
            dataset_name = self.configuration.dataset_name
            table_name = self.configuration.table_name
            columns = self.configuration.columns
            POINTER_PATH = self.configuration.pointer_path

            if not POINTER_PATH:
                raise ConfigurationException(
                    "POINTER_PATH is not set in the configuration."
                )

            for value in [project_id, dataset_name, table_name]:
                if not isinstance(value, str):
                    raise ConfigurationException(f"{value} must be a string")
            
            if not isinstance(columns, list):
                raise ConfigurationException(
                "columns must be a list."
        )
        except AttributeError as err:
            raise ConfigurationException(
                f"Missing required configuration attribute: {err}"
            )

        self.logger.info("BigQuery connector configured successfully.")

        try:
            client = bigquery.Client(credentials=self.get_credentials(), project=project_id)
            self.logger.debug("BigQuery client created successfully.")
        except Exception as e:
            self.logger.error(f"Failed to create BigQuery client: {e}")
            raise

        # If no pointer is stored, set it to a week ago
        try:
            pointer_epoch_ms = int(self.pointer)
            self.logger.debug(f"Pointer found: {pointer_epoch_ms} ({type(pointer_epoch_ms)})")
        except (NotFoundException, ValueError):
            pointer_epoch_ms = int((datetime.utcnow() - timedelta(days=7)).replace(tzinfo=timezone.utc).timestamp() * 1_000)

            self.logger.debug(f"No pointer found. Setting pointer to: {pointer_epoch_ms} ({type(pointer_epoch_ms)})")
            self.pointer = str(pointer_epoch_ms)

        str_pointer = as_bigquery_timestamp(pointer_epoch_ms)

        while True:
            self.logger.debug(f"Pointer for query: {str_pointer} ({type(str_pointer)})")

            query = f"""
            SELECT {', '.join(columns)}
            FROM `{project_id}.{dataset_name}.{table_name}`
            WHERE TIMESTAMP(_PARTITIONTIME) > TIMESTAMP('{str_pointer}')
            AND {self.POINTER_PATH} IS NOT NULL
            ORDER BY TIMESTAMP(_PARTITIONTIME) ASC
            LIMIT 1000
            """
            self.logger.debug(f"Constructed query: {query}")

            try:
                self.logger.info("Executing query on BigQuery.")
                query_job = client.query(query)
                results = query_job.result()
                self.logger.debug("Query executed successfully.")

                rows = [dict(row) for row in results]
                if not rows:
                    self.logger.info("No more logs found.")
                    break

                self.logger.info(f"Collected {len(rows)} logs.")
                self.save(rows)

                if len(rows) < 1000:
                    break

            except Exception as err:
                self.logger.error(f"BigQuery query failed: {err}")
                raise RequestFailedException(f"BigQuery query failed: {err}")

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
        # Subject not needed for Bigquery API
        try:
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
        except GoogleAuthError as err:  
            raise ConfigurationException(
                f"Authentication error while generating credentials for {self.identity}: {err}"
            )
        except ValueError as err: 
            raise ConfigurationException(
                f"Unable to generate credentials from service account info for {self.identity}: {err}"
            )

        return credentials
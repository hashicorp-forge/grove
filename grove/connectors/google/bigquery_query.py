# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Google BigQuery connector for Grove."""

import json
from datetime import datetime, timedelta, timezone

from google.auth.exceptions import GoogleAuthError
from google.oauth2 import service_account
from google.cloud import bigquery

from grove.connectors import BaseConnector
from grove.connectors.google.utils import as_bigquery_timestamp_microseconds
from grove.constants import CHRONOLOGICAL
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RequestFailedException,
)


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
            max_batches = getattr(self.configuration, 'max_batches', 3)
            self.POINTER_PATH = self.configuration.pointer_path

            self.logger.debug("Configuration parameters:")
            self.logger.debug(f"Project ID: {project_id}")
            self.logger.debug(f"Dataset Name: {dataset_name}")
            self.logger.debug(f"Table Name: {table_name}")
            self.logger.debug(f"Columns: {columns}")

            if not self.POINTER_PATH:
                raise ConfigurationException(
                    "POINTER_PATH is not set in the configuration."
                )
            
            if not isinstance(max_batches, int) or max_batches <= 0:
                raise ConfigurationException(
                    "max_batches must be a positive integer."
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
            pointer_epoch_usec = int(self.pointer)
            self.logger.debug(f"Pointer found: {pointer_epoch_usec} ({type(pointer_epoch_usec)})")
        except (NotFoundException, ValueError):
            # Set to a week ago in microseconds
            pointer_epoch_usec = int((datetime.utcnow() - timedelta(days=7)).replace(tzinfo=timezone.utc).timestamp() * 1_000_000)

            self.logger.debug(f"No pointer found. Setting pointer to: {pointer_epoch_usec} ({type(pointer_epoch_usec)})")
            self.pointer = str(pointer_epoch_usec)

        str_pointer = as_bigquery_timestamp_microseconds(pointer_epoch_usec)

        # Configuration for batching
        all_rows = []
        batch_count = 0

        while True:
            self.logger.debug(f"Pointer for query: {str_pointer} ({type(str_pointer)})")

            query = f"""
            SELECT {', '.join(columns)}
            FROM `{project_id}.{dataset_name}.{table_name}`
            WHERE {self.POINTER_PATH} > {str_pointer}
            AND {self.POINTER_PATH} IS NOT NULL
            ORDER BY {self.POINTER_PATH} ASC
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

                self.logger.info(f"Collected {len(rows)} logs in batch {batch_count + 1}.")
                all_rows.extend(rows)
                batch_count += 1

                # Save and break if we've collected enough batches or reached the end
                if batch_count >= max_batches or len(rows) < 1000:
                    if all_rows:
                        self.logger.debug(f"Saving {len(all_rows)} total logs from {batch_count} batches.")
                        self.save(all_rows)
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
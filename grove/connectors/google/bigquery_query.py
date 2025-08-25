# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Google BigQuery connector for Grove."""

import json
import time
from datetime import datetime, timedelta, timezone, date
from typing import Any, Optional, Tuple, List, Dict

from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery
from google.oauth2 import service_account

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
        """Collects logs from the specified table using Google BigQuery API with optimized partition pruning and pagination."""
        self.logger.info("Starting optimized data collection from BigQuery.")

        try:
            project_id = self.configuration.project_id
            dataset_name = self.configuration.dataset_name
            table_name = self.configuration.table_name
            columns = self.configuration.columns
            max_batches = getattr(self.configuration, "max_batches", 3)
            self.POINTER_PATH = self.configuration.pointer_path
            time_format = getattr(self.configuration, "time_format", "microseconds")
            
            # New configuration options for optimization
            page_size = getattr(self.configuration, "page_size", 5000)
            bootstrap_days = getattr(self.configuration, "bootstrap_days", 7)
            min_lookback_days = getattr(self.configuration, "min_lookback_days", 3)
            max_lookback_days = getattr(self.configuration, "max_lookback_days", 30)
            late_buffer_days = getattr(self.configuration, "late_buffer_days", 2)

            self.logger.debug("Configuration parameters:")
            self.logger.debug(f"Project ID: {project_id}")
            self.logger.debug(f"Dataset Name: {dataset_name}")
            self.logger.debug(f"Table Name: {table_name}")
            self.logger.debug(f"Columns: {columns}")
            self.logger.debug(f"Page Size: {page_size}")

            if not self.POINTER_PATH:
                raise ConfigurationException(
                    "POINTER_PATH is not set in the configuration."
                )

            if not isinstance(max_batches, int) or max_batches <= 0:
                raise ConfigurationException("max_batches must be a positive integer.")

            for value in [project_id, dataset_name, table_name]:
                if not isinstance(value, str):
                    raise ConfigurationException(f"{value} must be a string")

            if not isinstance(columns, list):
                raise ConfigurationException("columns must be a list.")

            if time_format not in ["microseconds", "timestamp"]:
                raise ConfigurationException(
                    "time_format must be either 'microseconds' or 'timestamp'"
                )

            if not isinstance(page_size, int) or page_size <= 0:
                raise ConfigurationException("page_size must be a positive integer.")

        except AttributeError as err:
            raise ConfigurationException(
                f"Missing required configuration attribute: {err}"
            )

        self.logger.info("BigQuery connector configured successfully.")

        # Create BigQuery client with retry logic for auth deadlock
        client = self._create_bigquery_client(project_id)

        # Initialize watermark and pointer
        last_seen_usec = self._initialize_watermark(time_format)
        
        # Configuration for batching
        all_rows = []
        batch_count = 0

        while batch_count < max_batches:
            # Compute adaptive lookback window
            now_utc = datetime.now(timezone.utc)
            lookback_days = self._compute_lookback_days(
                last_seen_usec, now_utc, bootstrap_days, min_lookback_days, 
                max_lookback_days, late_buffer_days
            )
            
            # Calculate partition bounds
            min_partition_date = (now_utc - timedelta(days=lookback_days)).date()
            ceiling_usec = int((now_utc - timedelta(seconds=180)).timestamp() * 1_000_000)  # Fixed 3-minute lag
            
            self.logger.debug(f"Fetching batch {batch_count + 1}:")
            self.logger.debug(f"  Lookback days: {lookback_days}")
            self.logger.debug(f"  Min partition date: {min_partition_date}")
            self.logger.debug(f"  Ceiling usec: {ceiling_usec}")
            self.logger.debug(f"  Last seen usec: {last_seen_usec}")

            # Fetch page with optimized query
            try:
                rows, new_last_seen_usec, debug_metadata = self._fetch_page_bigquery(
                    client=client,
                    project_id=project_id,
                    dataset_name=dataset_name,
                    table_name=table_name,
                    columns=columns,
                    pointer_path=self.POINTER_PATH,
                    time_format=time_format,
                    last_seen_usec=last_seen_usec,
                    page_size=page_size,
                    min_partition_date=min_partition_date,
                    ceiling_usec=ceiling_usec
                )
                
                if not rows:
                    self.logger.info("No more logs found.")
                    break

                self.logger.info(
                    f"Collected {len(rows)} logs in batch {batch_count + 1}. "
                    f"Debug: {debug_metadata}"
                )
                
                all_rows.extend(rows)
                batch_count += 1
                
                # Update watermark
                if new_last_seen_usec is not None:
                    last_seen_usec = new_last_seen_usec
                    # Update pointer for Grove's tracking
                    if time_format == "microseconds":
                        self.pointer = str(last_seen_usec)
                    else:
                        self.pointer = as_bigquery_timestamp_microseconds(last_seen_usec)
                
                # Check if we've reached the end (less than full page)
                if len(rows) < page_size:
                    self.logger.info("Reached end of available data.")
                    break
                    
            except Exception as err:
                self.logger.error(f"BigQuery query failed: {err}")
                raise RequestFailedException(f"BigQuery query failed: {err}")

        # Save collected rows
        if all_rows:
            self.logger.debug(
                f"Saving {len(all_rows)} total logs from {batch_count} batches."
            )
            self.save(all_rows)

    def _create_bigquery_client(self, project_id: str) -> bigquery.Client:
        """Create BigQuery client with retry logic for auth deadlock."""
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                client = bigquery.Client(
                    credentials=self.get_credentials(), project=project_id
                )
                self.logger.debug("BigQuery client created successfully.")
                return client
            except Exception as e:
                if "deadlock" in str(e).lower() or "ModuleLock" in str(e):
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Google Auth deadlock detected, retrying in {retry_delay} seconds (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                    else:
                        self.logger.error(f"Failed to create BigQuery client after {max_retries} attempts: {e}")
                        raise
                else:
                    self.logger.error(f"Failed to create BigQuery client: {e}")
                    raise
        
        # This should never be reached, but mypy requires it
        raise RuntimeError("Failed to create BigQuery client after all retry attempts")

    def _initialize_watermark(self, time_format: str) -> Optional[int]:
        """Initialize the watermark from stored pointer or set default."""
        try:
            stored_pointer = self.pointer
            self.logger.debug(
                f"Pointer found: {stored_pointer} ({type(stored_pointer)})"
            )

            if stored_pointer and stored_pointer.strip():
                if time_format == "microseconds":
                    try:
                        return int(stored_pointer)
                    except ValueError:
                        raise ConfigurationException(
                            f"Pointer '{stored_pointer}' is not a valid microseconds value"
                        )
                else:  # timestamp
                    try:
                        dt = datetime.fromisoformat(stored_pointer.replace("+00", "+00:00"))
                        return int(dt.timestamp() * 1_000_000)
                    except ValueError:
                        raise ConfigurationException(
                            f"Pointer '{stored_pointer}' is not a valid timestamp format"
                        )

        except (NotFoundException, ValueError, ConfigurationException):
            # Set to a week ago
            week_ago = datetime.utcnow() - timedelta(days=7)
            week_ago = week_ago.replace(tzinfo=timezone.utc)
            pointer_epoch_usec = int(week_ago.timestamp() * 1_000_000)
            
            # Update pointer for Grove's tracking
            if time_format == "microseconds":
                self.pointer = str(pointer_epoch_usec)
            else:
                self.pointer = as_bigquery_timestamp_microseconds(pointer_epoch_usec)
            
            self.logger.debug(
                f"No pointer found. Setting pointer to: {self.pointer}"
            )
            return pointer_epoch_usec
        
        # This should never be reached, but mypy requires it
        return None

    def _compute_lookback_days(
        self, 
        last_seen_usec: Optional[int], 
        now_utc: datetime,
        bootstrap_days: int = 7,
        min_days: int = 3,
        max_days: int = 30,
        late_buffer_days: int = 2
    ) -> int:
        """Compute adaptive lookback window based on how far behind we are."""
        if last_seen_usec is None:
            return bootstrap_days
        
        last_seen_dt = datetime.fromtimestamp(last_seen_usec / 1_000_000, tz=timezone.utc)
        delta_seconds = (now_utc - last_seen_dt).total_seconds()
        delta_days = max(0, delta_seconds / 86400)
        
        # Add buffer for late arrivals and clamp to bounds
        lookback_days = delta_days + late_buffer_days
        return max(min_days, min(max_days, int(lookback_days)))

    def _fetch_page_bigquery(
        self,
        client: bigquery.Client,
        project_id: str,
        dataset_name: str,
        table_name: str,
        columns: List[str],
        pointer_path: str,
        time_format: str,
        last_seen_usec: Optional[int],
        page_size: int,
        min_partition_date: date,
        ceiling_usec: int
    ) -> Tuple[List[Dict[str, Any]], Optional[int], Dict[str, Any]]:
        """
        Fetch a page of data using optimized BigQuery query with partition pruning.
        
        Returns:
            Tuple of (rows, new_last_seen_usec, debug_metadata)
        """
        # Set low watermark for keyset pagination
        low_watermark = last_seen_usec if last_seen_usec is not None else -1
        
        # Build query with parameters for better performance
        query = f"""
        SELECT {', '.join(columns)}
        FROM `{project_id}.{dataset_name}.{table_name}`
        WHERE _PARTITIONDATE >= @min_partition_date
        AND {pointer_path} > @low_watermark
        AND {pointer_path} <= @ceiling_usec
        ORDER BY {pointer_path} ASC
        LIMIT @page_size
        """
        
        self.logger.debug(f"Constructed query: {query}")
        
        # Set up query parameters
        query_params = [
            bigquery.ScalarQueryParameter("min_partition_date", "DATE", min_partition_date),
            bigquery.ScalarQueryParameter("low_watermark", "INT64", low_watermark),
            bigquery.ScalarQueryParameter("ceiling_usec", "INT64", ceiling_usec),
            bigquery.ScalarQueryParameter("page_size", "INT64", page_size),
        ]
        
        # Configure query job
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_params,
            use_query_cache=False  # Disable cache for consistent results
        )
        
        try:
            self.logger.info("Executing optimized BigQuery query.")
            query_job = client.query(query, job_config=job_config)
            results = query_job.result()
            self.logger.debug("Query executed successfully.")
            
            rows = [dict(row) for row in results]
            
            # Extract new watermark from last row
            new_last_seen_usec = None
            if rows:
                latest_row = rows[-1]
                # Navigate to the timestamp field using the pointer path
                timestamp_value: Any = latest_row
                for part in pointer_path.split('.'):
                    if isinstance(timestamp_value, dict):
                        timestamp_value = timestamp_value.get(part, None)
                    else:
                        break
                
                if timestamp_value is not None:
                    if time_format == "microseconds":
                        new_last_seen_usec = int(timestamp_value)
                    else:
                        # Convert timestamp to microseconds if needed
                        if isinstance(timestamp_value, (int, float)):
                            new_last_seen_usec = int(timestamp_value * 1_000_000)
                        else:
                            # Assume it's already a timestamp string, convert to microseconds
                            try:
                                dt = datetime.fromisoformat(str(timestamp_value).replace("+00", "+00:00"))
                                new_last_seen_usec = int(dt.timestamp() * 1_000_000)
                            except ValueError:
                                self.logger.warning(f"Could not parse timestamp value: {timestamp_value}")
                                new_last_seen_usec = last_seen_usec
            
            # Build debug metadata
            debug_metadata = {
                "lookback_days": (datetime.now(timezone.utc).date() - min_partition_date).days,
                "min_partition_date": str(min_partition_date),
                "ceiling_usec": ceiling_usec,
                "rows_returned": len(rows),
                "new_watermark": new_last_seen_usec,
                "bytes_processed": getattr(query_job, 'total_bytes_processed', 'unknown'),
                "slot_ms": getattr(query_job, 'slot_millis', 'unknown')
            }
            
            return rows, new_last_seen_usec, debug_metadata
            
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

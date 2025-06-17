"""Zendesk ticket audits connector for Grove."""

from typing import Any, Dict, List, Optional
import datetime
import json
import time
from datetime import datetime, timezone, timedelta

import requests
import logging

from grove.connectors import BaseConnector
from grove.exceptions import ConfigurationException, NotFoundException, RequestFailedException
from grove.models import ConnectorConfig
from grove.constants import CHRONOLOGICAL

DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_BATCH_SIZE = 100
DEFAULT_DELAY = 0
RATE_LIMIT_SECONDS = 60

class Client:
    """Zendesk API client for account audit logs."""

    def __init__(self, subdomain: str, api_token: str, email: str, batch_size: int, enforce_rate_limit: bool = False):
        """Initialize the client.

        :param subdomain: Zendesk subdomain.
        :param api_token: Zendesk API token.
        :param email: Zendesk email address.
        :param batch_size: Number of records to fetch per request (1-100).
        :param enforce_rate_limit: Whether to enforce the rate limit of 1 request per minute.
        """
        self.subdomain = subdomain
        self.api_token = api_token
        self.email = email
        self.batch_size = batch_size
        self.enforce_rate_limit = enforce_rate_limit
        self.base_url = f"https://{subdomain}.zendesk.com/api/v2"
        self.session = requests.Session()
        self.session.auth = (f"{email}/token", api_token)
        self.session.headers.update({
            "Content-Type": "application/json",
        })
        self.logger = logging.getLogger(__name__)
        self.last_request_time = 0  # Track last request time for rate limiting

    def _enforce_rate_limit(self):
        """Enforce rate limit of 1 request per minute if enabled."""
        if not self.enforce_rate_limit:
            return
            
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < RATE_LIMIT_SECONDS:
            sleep_time = RATE_LIMIT_SECONDS - time_since_last_request
            self.logger.info(f"Rate limit: Waiting {sleep_time:.2f} seconds before next request")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    def get_audit_logs(self, cursor: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get audit logs from Zendesk using cursor pagination."""
        self._enforce_rate_limit()
        
        url = f"{self.base_url}/audit_logs.json"
        params = {
            "page[size]": self.batch_size,
            "sort": "created_at"  
        }
        
        if start_date and end_date:
            params["filter[created_at][]"] = [start_date, end_date]
            self.logger.info(f"Filtering logs between {start_date} and {end_date}")
        
        if cursor:
            self.logger.info(f"Fetching next page with cursor: {cursor}")
            params["page[after]"] = cursor
        else:
            self.logger.info("Fetching first page of audit logs")

        response = self.session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        self.logger.info(f"Retrieved {len(data['audit_logs'])} audit logs")
        return data


class AuditLogsConnector(BaseConnector):
    """Zendesk account audit logs connector."""

    CONNECTOR = "zendesk_audit_logs"
    POINTER_PATH = "created_at"  # Use the created_at timestamp from each audit log object
    LOG_ORDER = CHRONOLOGICAL  # Use chronological order (oldest first)

    def __init__(self, config: ConnectorConfig, context: Dict[str, Any]):
        """Initialize the connector.

        :param config: Configuration for the connector.
        :param context: Runtime context for the connector.
        """
        super().__init__(config, context)
        self.subdomain = getattr(config, "subdomain", None)
        if not self.subdomain:
            raise ConfigurationException("subdomain is required")
        self.api_token = getattr(config, "key", None)
        if not self.api_token:
            raise ConfigurationException("key is required")
        self.email = getattr(config, "identity", None)
        if not self.email:
            raise ConfigurationException("identity is required")
            
        self.client = Client(
            self.subdomain, 
            self.api_token, 
            self.email, 
            self.batch_size,
            self.enforce_rate_limit
        )
        self.logger = logging.getLogger(__name__)

    @property
    def batch_size(self) -> int:
        """Get the configured batch size for API requests.
        
        :return: Number of records to fetch per request (1-100).
        :raises ConfigurationException: If batch_size is invalid.
        """
        try:
            candidate = getattr(self.config, "batch_size", DEFAULT_BATCH_SIZE)
            if not isinstance(candidate, int) or candidate <= 0 or candidate > 100:
                raise ConfigurationException("batch_size must be an integer between 1 and 100")
            return candidate
        except AttributeError:
            return DEFAULT_BATCH_SIZE

    @property
    def delay(self) -> int:
        """Get the configured delay in minutes.
        
        :return: Number of minutes to delay collection.
        :raises ConfigurationException: If delay is invalid.
        """
        try:
            candidate = int(getattr(self.config, "delay", DEFAULT_DELAY))
            if candidate < 0:
                raise ConfigurationException("delay must be a non-negative integer")
            return candidate
        except (AttributeError, ValueError):
            return DEFAULT_DELAY

    @property
    def enforce_rate_limit(self) -> bool:
        """Get whether rate limiting should be enforced.
        
        :return: True if rate limiting should be enforced, False otherwise.
        """
        try:
            candidate = getattr(self.config, "enforce_rate_limit", False)
            if candidate not in [True, False]:
                raise ConfigurationException("enforce_rate_limit must be a boolean")
            return candidate
        except AttributeError:
            return False

    def _get_time_range(self) -> tuple[datetime, datetime]:
        """Get the time range for log collection.
        
        :return: Tuple of (start_time, end_time) in UTC.
        """
        now = datetime.now(timezone.utc)
        end_time = now - timedelta(minutes=self.delay)
        
        try:
            # Try to parse the pointer as a datetime
            start_time = datetime.fromisoformat(self.pointer.replace('Z', '+00:00'))
            self.logger.info(f"Using existing pointer value: {start_time}")
        except NotFoundException:
            # If pointer doesn't exist, use 7 days ago
            start_time = now - timedelta(days=7)
            self.pointer = start_time.strftime(DATESTAMP_FORMAT)
            self.logger.info(f"No pointer found, using 7 days ago: {start_time}")
        
        return start_time, end_time

    def _is_log_newer_than_pointer(self, log: Dict[str, Any], start_time: datetime) -> bool:
        """Check if a log is newer than the pointer time.
        
        :param log: The log entry to check.
        :param start_time: The pointer time to compare against.
        :return: True if the log is newer than the pointer time.
        """
        log_time = datetime.fromisoformat(log["created_at"].replace('Z', '+00:00'))
        return log_time > start_time

    def _update_pointer(self, logs: List[Dict[str, Any]]) -> None:
        """Update the pointer to the most recent log timestamp.
        
        :param logs: List of log entries to check for the most recent timestamp.
        """
        if not logs:
            return
            
        # Find the most recent log timestamp
        latest_timestamp = max(
            datetime.fromisoformat(log["created_at"].replace('Z', '+00:00'))
            for log in logs
        )
        
        # Update pointer if this is newer than current pointer
        try:
            current_pointer = datetime.fromisoformat(self.pointer.replace('Z', '+00:00'))
            if latest_timestamp > current_pointer:
                self.pointer = latest_timestamp.strftime(DATESTAMP_FORMAT)
                self.logger.info(f"Updated pointer to: {self.pointer}")
        except (NotFoundException, ValueError):
            # If current pointer is invalid, always update
            self.pointer = latest_timestamp.strftime(DATESTAMP_FORMAT)
            self.logger.info(f"Set new pointer to: {self.pointer}")

    def collect(self) -> List[Dict[str, Any]]:
        """Collect account audit logs from Zendesk using cursor pagination."""
        cursor = None
        audit_logs = []
        page = 1
        SAVE_INTERVAL = 10  # Save every 10 pages
        last_cursor = None

        try:
            # Calculate the date range
            start_time, end_time = self._get_time_range()
            
            self.logger.info(f"Collecting logs between {start_time} and {end_time}")

        except Exception as e:
            self.logger.error(f"Error initializing audit log collection: {str(e)}")
            raise

        while True:
            self.logger.info(f"Fetching page {page}")
            try:
                response = self.client.get_audit_logs(
                    cursor, 
                    start_time.strftime(DATESTAMP_FORMAT), 
                    end_time.strftime(DATESTAMP_FORMAT)
                )
                audit_logs_batch = response["audit_logs"]
                
                if not audit_logs_batch:
                    self.logger.info("No more audit logs to collect")
                    break

                self.logger.info(f"API returned {len(audit_logs_batch)} audit logs on page {page}")
                
                # Log first and last audit log IDs and timestamps
                if audit_logs_batch:
                    first_log = audit_logs_batch[0]
                    last_log = audit_logs_batch[-1]
                    self.logger.info(f"First audit log ID in batch: {first_log['id']}")
                    self.logger.info(f"Last audit log ID in batch: {last_log['id']}")
                    self.logger.info(f"First audit log created_at: {first_log['created_at']}")
                    self.logger.info(f"Last audit log created_at: {last_log['created_at']}")

                # Filter logs newer than pointer using timezone-aware comparison
                new_logs = [log for log in audit_logs_batch if self._is_log_newer_than_pointer(log, start_time)]
                self.logger.info(f"Filtered {len(new_logs)} new logs from batch of {len(audit_logs_batch)}")
                audit_logs.extend(new_logs)
                
                self.logger.info(f"Total audit logs collected so far: {len(audit_logs)}")

                # Get next cursor from response
                if response["meta"]["has_more"]:
                    cursor = response["meta"]["after_cursor"]
                    self.logger.info(f"Next cursor will be: {cursor}")
                    
                    # Check if we got the same cursor as last time
                    if cursor == last_cursor:
                        self.logger.warning("Received same cursor as last page, stopping to avoid duplicates")
                        break
                    last_cursor = cursor
                else:
                    self.logger.info("No more pages available")
                    break

                # Save periodically to avoid memory issues
                if page % SAVE_INTERVAL == 0:
                    self.logger.info(f"Saving batch of {len(audit_logs)} audit logs")
                    self.save(audit_logs)
                    audit_logs = []

                page += 1

            except Exception as e:
                self.logger.error(f"Error collecting audit logs: {str(e)}")
                break

        # Save any remaining logs
        if audit_logs:
            self.logger.info(f"Saving final batch of {len(audit_logs)} audit logs")
            self.save(audit_logs)

        return audit_logs 
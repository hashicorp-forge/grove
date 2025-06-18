# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zendesk closed tickets connector for Grove."""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RateLimitException,
    RequestFailedException,
)
from grove.connectors.zendesk.api import ZendeskClient

DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Connector(BaseConnector):
    CONNECTOR = "zendesk_tickets"
    POINTER_PATH = "updated_at"
    LOG_ORDER = CHRONOLOGICAL

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = ZendeskClient(
            subdomain=self.subdomain,
            identity=self.identity,
            api_token=self.key
        )

    @property
    def subdomain(self) -> str:
        """Fetches the Zendesk subdomain from the configuration.

        :return: The "subdomain" component of the connector configuration.
        :raises ConfigurationException: If subdomain is not configured.
        """
        try:
            return self.configuration.subdomain
        except AttributeError:
            raise ConfigurationException(
                "Zendesk subdomain is required but not configured."
            )

    @property
    def include_comments(self) -> bool:
        """Whether to include ticket comments in the response.

        :return: The "include_comments" component of the connector configuration.
        """
        try:
            return self.configuration.include_comments
        except AttributeError:
            return True

    @property
    def include_attachments(self) -> bool:
        """Whether to include attachments in comments.

        :return: The "include_attachments" component of the connector configuration.
        """
        try:
            return self.configuration.include_attachments
        except AttributeError:
            return True

    @property
    def delay_minutes(self) -> int:
        """Number of minutes to delay collection to ensure data consistency.
        
        This accounts for potential delays in Zendesk's data pipeline.
        """
        try:
            return int(self.configuration.delay_minutes)
        except (AttributeError, ValueError):
            return 5  # Default 5 minute delay

    @property
    def batch_size(self) -> int:
        """Number of tickets to process in each batch.
        
        Smaller batches provide better progress tracking and allow for
        intermediate saves, but may increase overall runtime slightly.
        """
        try:
            return int(self.configuration.batch_size)
        except (AttributeError, ValueError):
            return 50  # Default batch size

    def _get_tickets_since(self, start_time: datetime) -> List[Dict[str, Any]]:
        """Get all tickets updated since the specified time using ZendeskClient."""
        tickets = []
        start_timestamp = int(start_time.timestamp())
        cursor = None
        while True:
            data = self._client.get_incremental_tickets(start_timestamp, cursor)
            page_tickets = data.get("tickets", [])
            tickets.extend(page_tickets)
            if data.get("end_of_stream", False):
                break
            cursor = data.get("after_cursor")
            if not cursor:
                break
            time.sleep(1)
        return tickets

    def _get_ticket_comments(self, ticket_id: int) -> List[Dict[str, Any]]:
        """Get all comments for a specific ticket using ZendeskClient."""
        return self._client.get_ticket_comments(ticket_id, include_inline_images=self.include_attachments)

    def _filter_closed_tickets(self, tickets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter tickets to only include closed ones.

        :param tickets: List of all tickets.
        :return: List of closed tickets.
        """
        closed_statuses = ["closed", "solved"]
        closed_tickets = [
            ticket for ticket in tickets 
            if ticket.get("status") in closed_statuses
        ]
        
        self.logger.info(
            f"Filtered to {len(closed_tickets)} closed tickets from {len(tickets)} total",
            extra=self.log_context
        )
        
        return closed_tickets

    def _enrich_tickets_with_comments(self, tickets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich tickets with their comments and attachments.

        :param tickets: List of tickets to enrich.
        :return: List of enriched tickets.
        """
        enriched_tickets = []
        
        for i, ticket in enumerate(tickets):
            ticket_id = ticket["id"]
            
            self.logger.debug(
                f"Enriching ticket {ticket_id} with comments ({i+1}/{len(tickets)})",
                extra={
                    "ticket_id": ticket_id, 
                    "progress": f"{i+1}/{len(tickets)}",
                    "progress_percent": round((i+1)/len(tickets)*100, 1),
                    **self.log_context
                }
            )
            
            # Get comments for this ticket
            if self.include_comments:
                try:
                    comments = self._get_ticket_comments(ticket_id)
                    ticket["comments"] = comments
                    
                    # Count attachments if present
                    attachment_count = 0
                    for comment in comments:
                        attachments = comment.get("attachments", [])
                        attachment_count += len(attachments)
                    
                    self.logger.debug(
                        f"Retrieved {len(comments)} comments with {attachment_count} attachments",
                        extra={
                            "ticket_id": ticket_id,
                            "comment_count": len(comments),
                            "attachment_count": attachment_count,
                            **self.log_context
                        }
                    )
                    
                except Exception as err:
                    self.logger.warning(
                        f"Failed to get comments for ticket {ticket_id}: {err}",
                        extra={"ticket_id": ticket_id, "exception": err, **self.log_context}
                    )
                    # Continue without comments rather than failing
                    ticket["comments"] = []
            
            enriched_tickets.append(ticket)
            
            # Reduced rate limiting - only sleep every 5 requests
            if (i + 1) % 5 == 0:
                time.sleep(0.1)  # Reduced from 0.2s per request
        
        return enriched_tickets

    def collect(self):
        """Collects closed tickets with comments and attachments from Zendesk.

        This will first check for any cached pointers to indicate previous collections.
        If not found, it will start from one week ago.
        """
        # Calculate start time with delay for data consistency
        now = datetime.now(timezone.utc)
        delayed_now = now - timedelta(minutes=self.delay_minutes)
        
        # Determine the start time for collection
        try:
            # Try to get existing pointer from cache
            pointer_value = self.pointer
            # Parse the existing pointer as an ISO timestamp
            start_time = datetime.fromisoformat(
                pointer_value.replace("Z", "+00:00")
            )
            self.logger.info(
                "Found existing pointer, continuing from last collection",
                extra={"start_time": start_time.isoformat(), **self.log_context}
            )
        except (NotFoundException, ValueError):
            # No previous run, start from 7 days ago and set initial pointer
            start_time = delayed_now - timedelta(days=7)
            # Initialize pointer to avoid cache issues later
            self.pointer = start_time.isoformat()
            self.logger.info(
                "No previous collection found, starting from 7 days ago",
                extra={"start_time": start_time.isoformat(), **self.log_context}
            )

        self.logger.info(
            "Starting Zendesk ticket collection",
            extra={
                "start_time": start_time.isoformat(),
                "delayed_until": delayed_now.isoformat(),
                **self.log_context
            }
        )

        # Get all tickets since start time
        all_tickets = self._get_tickets_since(start_time)
        
        if not all_tickets:
            self.logger.info(
                "No tickets found in time range",
                extra=self.log_context
            )
            return

        # Filter to only closed tickets
        closed_tickets = self._filter_closed_tickets(all_tickets)
        
        if not closed_tickets:
            self.logger.info(
                "No closed tickets found in time range",
                extra=self.log_context
            )
            # Still update pointer to mark progress
            self.pointer = delayed_now.isoformat()
            return

        # Process tickets in batches for better progress tracking and intermediate saves
        batch_size = self.batch_size
        total_batches = (len(closed_tickets) + batch_size - 1) // batch_size
        
        self.logger.info(
            f"Processing {len(closed_tickets)} closed tickets in {total_batches} batches of {batch_size}",
            extra={
                "total_tickets": len(closed_tickets),
                "batch_size": batch_size,
                "total_batches": total_batches,
                **self.log_context
            }
        )

        all_enriched_tickets = []
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(closed_tickets))
            batch_tickets = closed_tickets[start_idx:end_idx]
            
            self.logger.info(
                f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_tickets)} tickets)",
                extra={
                    "batch_number": batch_num + 1,
                    "batch_size": len(batch_tickets),
                    "progress_percent": round((batch_num + 1) / total_batches * 100, 1),
                    **self.log_context
                }
            )
            
            # Enrich this batch with comments and attachments
            enriched_batch = self._enrich_tickets_with_comments(batch_tickets)
            all_enriched_tickets.extend(enriched_batch)
            
            # Save intermediate progress every batch
            if enriched_batch:
                self.save(enriched_batch)
                self.logger.info(
                    f"Saved batch {batch_num + 1}/{total_batches} - {len(enriched_batch)} tickets",
                    extra={
                        "batch_number": batch_num + 1,
                        "batch_tickets_saved": len(enriched_batch),
                        "total_tickets_processed": len(all_enriched_tickets),
                        **self.log_context
                    }
                )

        self.logger.info(
            f"Successfully collected {len(all_enriched_tickets)} closed tickets",
            extra={
                "ticket_count": len(all_enriched_tickets),
                "total_tickets_checked": len(all_tickets),
                **self.log_context
            }
        )

        # Update pointer to mark completion
        self.pointer = delayed_now.isoformat()

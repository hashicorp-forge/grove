# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zendesk Search API connector for Grove.

This connector utilizes the Zendesk Search API to retrieve closed tickets
with their comments and attachments.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List


from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
)
from grove.connectors.zendesk.api import ZendeskClient

DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Connector(BaseConnector):
    CONNECTOR = "zendesk_search"
    POINTER_PATH = "updated_at"
    LOG_ORDER = CHRONOLOGICAL

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
    def ticket_status(self) -> str:
        """The ticket status to filter on.

        :return: The "status" component of the connector configuration.
        """
        try:
            return self.configuration.status
        except AttributeError:
            return "closed"  # Default to closed tickets

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
    def delay_minutes(self) -> int:
        """Delay in minutes to allow for data consistency.

        :return: The "delay_minutes" component of the connector configuration.
        """
        try:
            return int(self.configuration.delay_minutes)
        except (AttributeError, ValueError):
            return 5

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = ZendeskClient(
            subdomain=self.subdomain,
            identity=self.identity,
            api_token=self.key
        )

    def _build_search_query(self, start_time: datetime) -> str:
        """Build the search query for tickets.

        :param start_time: The start time for the search.
        :return: The search query string.
        """
        # Format the start time for Zendesk search
        start_time_str = start_time.strftime("%Y-%m-%d")
        
        # Build the query
        query_parts = [
            "type:ticket",
            f"status:{self.ticket_status}",
            f"updated>={start_time_str}"
        ]
        
        query = " ".join(query_parts)
        
        self.logger.info(
            f"Built search query: {query}",
            extra={"query": query, **self.log_context}
        )
        
        return query

    def collect(self):
        """Collects closed tickets with comments and attachments from Zendesk.

        This uses the Zendesk Search API to find tickets matching the specified criteria,
        then enriches each ticket with its comments and attachments.
        """
        # Calculate start time with delay for data consistency
        now = datetime.now(timezone.utc)
        delayed_now = now - timedelta(minutes=self.delay_minutes)
        
        # Determine the start time for collection
        try:
            # Parse the existing pointer as an ISO timestamp
            start_time = datetime.fromisoformat(
                self.pointer.replace("Z", "+00:00")
            )
        except (NotFoundException, ValueError):
            # No previous run, start from 7 days ago
            start_time = delayed_now - timedelta(days=7)
            self.logger.info(
                "No previous collection found, starting from 7 days ago",
                extra={"start_time": start_time.isoformat(), **self.log_context}
            )

        self.logger.info(
            "Starting Zendesk ticket search collection",
            extra={
                "start_time": start_time.isoformat(),
                "delayed_until": delayed_now.isoformat(),
                "status_filter": self.ticket_status,
                **self.log_context
            }
        )

        # Build and execute the search query
        query = self._build_search_query(start_time)
        tickets = self._client.search_tickets(query)
        
        if not tickets:
            self.logger.info(
                "No tickets found matching search criteria",
                extra={"query": query, **self.log_context}
            )
            # Still update pointer to mark progress
            self.pointer = delayed_now.isoformat()
            return

        self.logger.info(
            f"Found {len(tickets)} tickets matching search criteria",
            extra={"ticket_count": len(tickets), **self.log_context}
        )

        # Enrich with comments and attachments
        enriched_tickets = self._enrich_tickets_with_comments(tickets)

        # Save the collected data
        self.save(enriched_tickets)

        # Update the pointer to the current delayed time
        self.pointer = delayed_now.isoformat()

        self.logger.info(
            f"Successfully collected {len(enriched_tickets)} tickets",
            extra={
                "ticket_count": len(enriched_tickets),
                "pointer_updated": delayed_now.isoformat(),
                **self.log_context
            }
        )

    def _enrich_tickets_with_comments(self, tickets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich tickets with their comments and attachments.

        :param tickets: List of tickets to enrich.
        :return: List of enriched tickets.
        """
        enriched_tickets = []
        
        for ticket in tickets:
            ticket_id = ticket["id"]
            
            self.logger.debug(
                f"Enriching ticket {ticket_id} with comments",
                extra={"ticket_id": ticket_id, **self.log_context}
            )
            
            # Get comments for this ticket
            if self.include_comments:
                try:
                    comments = self._client.get_ticket_comments(ticket_id)
                    ticket["comments"] = comments
                    
                    # Count attachments if present
                    attachment_count = sum(
                        len(comment.get("attachments", []))
                        for comment in comments
                    )
                    
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
            
            # Rate limiting between ticket comment requests
            time.sleep(0.2)
        
        return enriched_tickets

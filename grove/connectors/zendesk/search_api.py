# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zendesk Search API connector for Grove.

This connector utilizes the Zendesk Search API to retrieve closed tickets
with their comments and attachments.
"""

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

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make an authenticated request to the Zendesk API.

        :param url: The API endpoint URL.
        :param params: Optional query parameters.
        :return: JSON response data.
        :raises RequestFailedException: If the request fails.
        :raises RateLimitException: If rate limited.
        """
        auth = (f"{self.identity}/token", self.key)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            response = requests.get(
                url, 
                auth=auth, 
                headers=headers, 
                params=params or {},
                timeout=30
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                raise RateLimitException(
                    f"Rate limited. Retry after {retry_after} seconds."
                )

            if response.status_code != 200:
                raise RequestFailedException(
                    f"Request failed with status {response.status_code}: {response.text}"
                )

            return response.json()

        except requests.exceptions.RequestException as err:
            raise RequestFailedException(f"Request failed: {err}")

    def _search_tickets(self, query: str) -> List[Dict[str, Any]]:
        """Search for tickets using the Zendesk Search API.

        :param query: The search query to execute.
        :return: List of ticket results.
        """
        tickets = []
        url = f"https://{self.subdomain}.zendesk.com/api/v2/search.json"
        
        page = 1
        per_page = 100
        
        while True:
            params = {
                "query": query,
                "page": page,
                "per_page": per_page,
                "sort_by": "updated_at",
                "sort_order": "asc"
            }
            
            self.logger.debug(
                f"Searching tickets with query: {query}",
                extra={"page": page, "per_page": per_page, **self.log_context}
            )
            
            data = self._make_request(url, params)
            
            page_results = data.get("results", [])
            
            # Filter only ticket results (search can return other types)
            page_tickets = [
                result for result in page_results 
                if result.get("result_type") == "ticket"
            ]
            
            tickets.extend(page_tickets)
            
            self.logger.info(
                f"Retrieved {len(page_tickets)} tickets from page {page}",
                extra={"total_so_far": len(tickets), **self.log_context}
            )
            
            # Check if there are more pages
            if not data.get("next_page") or len(page_tickets) < per_page:
                break
                
            page += 1
            
            # Rate limiting between pages
            time.sleep(1)

        return tickets

    def _get_ticket_comments(self, ticket_id: int) -> List[Dict[str, Any]]:
        """Get all comments for a specific ticket.

        :param ticket_id: The ticket ID to get comments for.
        :return: List of comment data including attachments.
        """
        comments = []
        url = f"https://{self.subdomain}.zendesk.com/api/v2/tickets/{ticket_id}/comments.json"
        
        params = {"include_inline_images": "true"}

        while True:
            data = self._make_request(url, params)
            
            page_comments = data.get("comments", [])
            comments.extend(page_comments)
            
            # Check pagination
            next_page = data.get("next_page")
            if not next_page:
                break
                
            url = next_page
            params = {}  # Next page URL includes all params
            
            # Rate limiting between comment pages
            time.sleep(0.5)

        return comments

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
                    comments = self._get_ticket_comments(ticket_id)
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
        tickets = self._search_tickets(query)
        
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

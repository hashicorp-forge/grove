# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zendesk API client for Grove connectors."""

import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

from grove.exceptions import RateLimitException, RequestFailedException


class ZendeskClient:
    """A client for interacting with the Zendesk API."""

    def __init__(self, subdomain: str, identity: str, api_token: str):
        """Initialize the Zendesk API client.

        :param subdomain: The Zendesk subdomain.
        :param identity: The identity (email address) for authentication.
        :param api_token: The API token for authentication.
        """
        self.base_url = f"https://{subdomain}.zendesk.com/api/v2/"
        self.auth = (f"{identity}/token", api_token)
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make an authenticated request to the Zendesk API.

        :param endpoint: The API endpoint (relative to base_url).
        :param params: Optional query parameters.
        :return: JSON response data.
        :raises RequestFailedException: If the request fails.
        :raises RateLimitException: If rate limited.
        """
        url = urljoin(self.base_url, endpoint)
        
        try:
            response = requests.get(
                url, 
                auth=self.auth, 
                headers=self.headers, 
                params=params,
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

    def search_tickets(self, query: str, per_page: int = 100) -> List[Dict[str, Any]]:
        """Search for tickets using the Zendesk Search API.

        :param query: The search query to execute.
        :param per_page: Number of results per page.
        :return: List of ticket results.
        """
        tickets = []
        page = 1
        
        while True:
            params = {
                "query": query,
                "page": page,
                "per_page": per_page,
                "sort_by": "updated_at",
                "sort_order": "asc"
            }
            
            data = self._make_request("search.json", params)
            page_results = data.get("results", [])
            
            # Filter only ticket results (search can return other types)
            page_tickets = []
            for result in page_results:
                if result.get("result_type") == "ticket":
                    page_tickets.append(result)
            
            tickets.extend(page_tickets)
            
            # Check if there are more pages
            if not data.get("next_page") or len(page_tickets) < per_page:
                break
                
            page += 1
            time.sleep(1)  # Rate limiting

        return tickets

    def get_ticket_comments(self, ticket_id: int, include_inline_images: bool = True) -> List[Dict[str, Any]]:
        """Get all comments for a specific ticket.

        :param ticket_id: The ticket ID to get comments for.
        :param include_inline_images: Whether to include inline images.
        :return: List of comment data including attachments.
        """
        comments = []
        endpoint = f"tickets/{ticket_id}/comments.json"
        
        params = {}
        if include_inline_images:
            params["include_inline_images"] = "true"

        while True:
            data = self._make_request(endpoint, params)
            page_comments = data.get("comments", [])
            comments.extend(page_comments)
            
            # Check pagination
            next_page = data.get("next_page")
            if not next_page:
                break
                
            # For next page, we need to use the full URL
            endpoint = next_page.replace(self.base_url, "")
            params = {}  # Next page URL includes all params
            
            time.sleep(0.5)  # Rate limiting

        return comments

    def get_incremental_tickets(self, start_time: int, cursor: Optional[str] = None) -> Dict[str, Any]:
        """Get tickets using the incremental export API.

        :param start_time: Unix timestamp to start from.
        :param cursor: Optional cursor for pagination.
        :return: API response with tickets and pagination info.
        """
        endpoint = "incremental/tickets/cursor.json"
        
        if cursor:
            params = {"cursor": cursor}
        else:
            params = {"start_time": start_time}
        
        return self._make_request(endpoint, params) 
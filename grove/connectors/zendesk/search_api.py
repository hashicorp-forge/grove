# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Zendesk API client.

This utilizes the Zendesk Search API to retrieve tickets.

Configuration object will require a ticket status to filter on
"""

import logging
import time
from typing import Any, Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

SEARCH_API_BASE_URI = "/api/v2/search?query={query}"

API_BASE_URI = "https://{base_url}/ccx/api/privacy/v1/{identity}"
API_PAGE_SIZE = 100\

# EXAMPLE: query for resource type "ticket" where status is "closed"
query = "type%3Aticket+status%3Aclosed"

# use this script to boilerplate access to tickets, users, orgs, groups
# use tickets.py to get all tickets (i.e. all closed tickets)
# use comments.py to get all comments for each ticket

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""GitHub API client.

As PyGitHub does not currently support the Audit API, this client has been created in
the interim.
"""

import datetime
import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse


class Client:
    def __init__(
        self,
        hostname: str = "api.github.com",
        scope: str = "orgs",
        identity: Optional[str] = None,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
    ):
        """Setup a new GitHub audit log client.

        :param hostname: Hostname of the GitHub API to interact with.
        :param identity: Name of the GitHub identity to collect audit logs for.
        :param token: Personal Access Token (PAT) to authenticate with.
        :param scope: The scope of the user - such as "orgs" or "enterprises".
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        self.retry = retry
        self.scope = scope
        self.hostname = hostname
        self.identity = identity
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {token}",
        }

    def _parse_link_header(self, link: str) -> str:
        """Attempt to parse the "next" URL from a provided Link header.

        :param link: Value of a Link header returned from a previous request.

        :raises ValueError: No, or an invalid, Link header was encountered.

        :return: Extracted "Next" URL from the provided Link header.
        """
        # A link header may contain N entries ("first", "next", and "last").
        url = None
        links = link.split(",")

        for entry in links:
            link_parts = entry.split(";")
            link_rel = link_parts[-1].strip()

            # There may not always be 'next' - such as in the case of the LAST page of
            # results.
            if "next" in link_rel:
                url = unquote(link_parts[0].strip().lstrip("<").rstrip(">"))

        # Very likely the last page.
        if not url:
            raise ValueError()

        # Try to mitigate SSRFs where a baked Link header is returned.
        if self.hostname.lower() not in url.lower():
            raise ValueError(
                f"{self.hostname} not found in Link header ({url}). Ignoring."
            )

        return url

    def _get(
        self,
        url: str,
        params: Optional[Dict[str, Optional[str]]] = None,
    ) -> HTTPResponse:
        """A GET wrapper to handle retries for the caller.

        :param url: URL to perform the HTTP GET against.
        :param params: HTTP parameters to add to the request.

        :raises RateLimitException: A rate limit was encountered.
        :raises RequestFailedException: An HTTP request failed.

        :return: HTTP Response object containing the headers and body of a response.
        """
        while True:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as err:
                # 403s OR 429s are used with a header to indicate rate-limit exceeded.
                if int(getattr(err.response, "status_code", 0)) not in [403, 429]:
                    raise RequestFailedException(err)

                if err.response.headers.get("X-RateLimit-Remaining") != "0":
                    raise RequestFailedException(err)

                # Retry on rate-limit, but only if requested.
                self.logger.warning("Rate-limit was exceeded during request")
                if not self.retry:
                    raise RateLimitException(err)

                time_current = int(
                    datetime.datetime.now(datetime.timezone.utc).strftime("%s")
                )
                time_ratelimit_reset = int(
                    err.response.headers.get("X-RateLimit-Reset", time_current)
                )
                time_wait = time_ratelimit_reset - time_current

                # If the rate-limit retry is greater than a few of minutes, just bail as
                # we'll pick back up at the next execution.
                if time_wait >= 180:
                    raise RateLimitException(err)

                # Only wait for a second if the retry time was unset or in the past.
                if time_wait > 0:
                    time.sleep(time_wait)
                else:
                    time.sleep(1)

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_rulesets(
        self,
        after: Optional[str] = None,
        time_period: Optional[str] = "day",
        rule_suite_result: Optional[str] = "all",
    ) -> List[str]:
        """Fetches a list of rulesets identifiers from the Github API.

        This method does not return audit log entries, nor results per page. Instead it
        will attempt to fetch ALL relevant ruleset identifiers for the configured
        organisation and repository (if configured).

        An API call per ruleset identifier must be performed to fetch the data around
        which rules were evaluated, and their outcomes.

        As a result of the above, and as a result of the lack of filtering on this
        endpoint this is an expensive operation.

        :param time_period: The time period to request data for (hour, day, week, etc).
        :param rule_suite_result: The result type to filter by (defaults to 'all').

        :return: A list of strings, containing the ruleset identifiers."""
        cursor = None
        rulesets = []

        # We perform client side filtering to only include new data, as each request
        # we have to process a day worth of data - due to the design of this API.
        more_requests = True

        while more_requests:
            if cursor is None:
                result = self._get(
                    f"https://{self.hostname}/{self.scope}/{self.identity}/rulesets/rule-suites",  # noqa: E501
                    params={
                        "rule_suite_result": rule_suite_result,
                        "time_period": time_period,
                        "per_page": "100",
                    },
                )
            else:
                self.logger.debug(
                    "Collecting next page with provided cursor",
                    extra={"cursor": cursor},
                )
                result = self._get(cursor)

            # Grab a list of all ruleset identifiers.
            for entry in result.body:
                entry["owner"] = self.scope # add owner to entry

                rulesets.append(entry.get("id"))

                if after:
                    # The data returned from the API is in reverse chronological order,
                    # so we will collect all of the rulesets up to and including the
                    # value we have previously seen - or all available data for the
                    # period if we never encounter that value.
                    pushed_at = entry.get("pushed_at")
                    if pushed_at == after:
                        more_requests = False
                        break

            # Return the results if no more data is available.
            try:
                cursor = self._parse_link_header(result.headers.get("Link", ""))
            except ValueError:
                break

        return rulesets

    def get_rule_suite(self, rule_suite_id: str) -> Dict[str, Any]:
        """Fetches the rule-suite information by identifier.

        :param rule_suite_id: The rule-suite identifier to return data for.

        :return: A dictionary containing the rule-suite information from Github.
        """
        try:
            result = self._get(
                f"https://{self.hostname}/{self.scope}/{self.identity}/rulesets/rule-suites/{rule_suite_id}",  # noqa: E501
            )
        except RequestFailedException as err:
            if hasattr(err, "response"):
                # Don't fail the entire batch if the rule-suite can't be found, just log
                # and continue.
                if err.response.status_code == 404:
                    self.logger.warning(
                        f"Could not get rule-suite for '{rule_suite_id}', skipping",
                    )
                    return {}

            raise err

        return result.body

    def get_audit_log(
        self,
        phrase: Optional[str] = None,
        include: Optional[str] = "all",
        order: Optional[str] = "asc",
        cursor: Optional[str] = None,
    ) -> AuditLogEntries:
        """Fetches a list of audit logs which match the requested filter and event type.

        :param phrase: Search phrase to use when filtering logs.
        :param include: Event type to return (web, git, or all).
        :param order: Order to return results (asc, desc).
        :param cursor: Cursor to use when fetching results. Supersedes other parameters.

        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        # Use the cursor URL if set, otherwise construct the initial query.
        if cursor is not None:
            self.logger.debug(
                "Collecting next page with provided cursor", extra={"cursor": cursor}
            )
            result = self._get(cursor)
        else:
            # See psf/requests issue #2651 for why we can happily pass in None values
            # and not have the request key added to the URI.
            result = self._get(
                f"https://{self.hostname}/{self.scope}/{self.identity}/audit-log",
                params={
                    "phrase": phrase,
                    "include": include,
                    "order": order,
                    "per_page": "100",
                },
            )

        # Track the results.
        try:
            cursor = self._parse_link_header(result.headers.get("Link", ""))
        except ValueError:
            cursor = None

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body)  # type: ignore

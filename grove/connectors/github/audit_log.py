# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""GitHub Audit connector for Grove."""

from datetime import datetime, timedelta, timezone

from grove.connectors import BaseConnector
from grove.connectors.github.api import Client
from grove.constants import CHRONOLOGICAL
from grove.exceptions import ConfigurationException, NotFoundException

DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Connector(BaseConnector):
    NAME = "github_audit_log"
    LOG_ORDER = CHRONOLOGICAL

    # Double quoting is required so that jmespath understands that @timestamp is the
    # field to be extracted - due to the special character at the start ('@').
    POINTER_PATH = '"@timestamp"'

    @property
    def delay(self):
        """Defines the amount of time to delay collection of logs (in minutes).

        This is used to allow time for logs to become 'consistent' before they are
        collected. This is required as Github backfills log entries but unfortunately do
        not provide any guidance around 'lag' time, or guarantees on availability and
        delivery.

        As a result of these constraints, this value is configurable to allow operators
        to preference consistency over speed of delivery, and vice versa. For example,
        a delay of 20 would instruct Grove to only collect logs after they are at least
        20 minutes old.

        This defaults to 0 (no delay).

        :return: The "delay" component of the connector configuration.
        """
        try:
            candidate = self.configuration.delay
        except AttributeError:
            return 0

        try:
            candidate = int(candidate)
        except ValueError as err:
            raise ConfigurationException(
                f"Configured 'delay' is not valid. Value must be an integer. {err}"
            )

        return candidate

    @property
    def scope(self):
        """Fetches the configured Github scope.

        This is used to control whether the connector should collect logs for a Github
        enterprise, or an organisation. This defaults to "orgs".

        :return: The "scope" component of the connector configuration.
        """
        try:
            candidate = self.configuration.scope
        except AttributeError:
            return "orgs"

        # Check that this style of account is supported.
        SUPPORTED = ["enterprises", "orgs"]

        if candidate.lower() not in SUPPORTED:
            raise ConfigurationException(
                f"Configured 'scope' is not valid. Only {SUPPORTED} are supported."
            )

        return candidate

    @property
    def fqdn(self):
        """Fetches the configured Github API FQDN, or the default (SaaS).

        :return: The "fqdn" component of the connector configuration.
        """
        try:
            return self.configuration.fqdn
        except AttributeError:
            return "api.github.com"

    def collect(self):
        """Collects all logs from the GitHub Audit API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        client = Client(
            token=self.key,
            scope=self.scope,
            identity=self.identity,
            hostname=self.fqdn,
        )
        cursor = None

        # If no pointer is stored then a previous run hasn't been performed, so set the
        # pointer to a week ago. In the case of the GitHub audit API the pointer is the
        # value of the "created_at" field from the latest record retrieved from the
        # API - which is in milliseconds since epoch format.
        try:
            _ = self.pointer
        except NotFoundException:
            # Precision doesn't matter too much here, as the GitHub API currently
            # doesn't appear to support millisecond granularity in filters.
            self.pointer = str(
                int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
            )

        # Transform the pointer into an ISO8601 compatible date and construct the search
        # phrase.
        start = datetime.utcfromtimestamp(int(self.pointer) / 1000)
        end = datetime.utcnow() - timedelta(minutes=self.delay)

        # Get log data from the upstream API, paging as required.
        while True:
            if end <= start:
                self.logger.debug(
                    "Collection end time is prior to start, skipping.",
                    extra={
                        "start": start.strftime(DATESTAMP_FORMAT),
                        "end": end.strftime(DATESTAMP_FORMAT),
                    },
                )
                break

            log = client.get_audit_log(
                phrase=(
                    f"created:>={start.strftime(DATESTAMP_FORMAT)} "
                    f"created:<={end.strftime(DATESTAMP_FORMAT)}"
                ),
                include=self.operation,
                cursor=cursor,
            )

            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            cursor = log.cursor
            if cursor is None:
                break

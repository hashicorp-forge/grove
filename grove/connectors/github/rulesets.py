# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""GitHub Ruleset connector for Grove."""

from typing import Any, List

from grove.connectors import BaseConnector
from grove.connectors.github.api import Client
from grove.constants import REVERSE_CHRONOLOGICAL
from grove.exceptions import (
    ConfigurationException,
    NotFoundException,
    RequestFailedException,
)

BATCH_SIZE = 100
DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Connector(BaseConnector):
    CONNECTOR = "github_rulesets"
    LOG_ORDER = REVERSE_CHRONOLOGICAL
    POINTER_PATH = "pushed_at"

    @property
    def time_period(self):
        """The time period to request data from the Github API for.

        This API only supports specific values for this value, rather than timestamps.
        This connector defaults to "day", but values supported are "hour", "day",
        "week", and "month".

        Please note that the use of longer periods on busy organisations is likely to
        result in rate-limits being hit due to the design of this API.

        :return: The "time_period" section of the connector configuration.
        """
        try:
            accepted = ["hour", "day", "month", "year"]
            candidate = self.configuration.time_period

            if candidate.lower() not in accepted:
                raise ConfigurationException(
                    "Configured 'time_period' is not valid. Value must be one of "
                    f"{accepted} values."
                )
        except AttributeError:
            return "day"

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
        """Collects ruleset data from the Github API.

        This API does not allow filtering based on time. It also requires an API call
        per ruleset identifier which data needs to be returned for, as a result it will
        result in a large number of HTTP requests when run, and can exhaust API limits
        quite rapidly.
        """
        client = Client(
            token=self.key,
            identity=self.identity,
            hostname=self.fqdn,
        )

        # Pointers are a bit odd here, as this API doesn't allow filtering by timestamp.
        # As a result, we need to do client side filtering, after requesting data for
        # the past day.
        try:
            after = self.pointer
        except NotFoundException:
            after = None

        # We always need to start by requesting a list of all rule-set identifiers
        # to request data about.
        rulesets = client.get_rulesets(
            after=after,
            rule_suite_result=self.operation,
        )

        # Batch records into appropriate sized chunks ourselves, to try and ensure logs
        # are flushed periodically.
        entries: List[Any] = []

        for ruleset in rulesets:
            try:
                record = client.get_rule_suite(rule_suite_id=ruleset)
            except RequestFailedException as err:
                self.save(entries)
                raise err

            if record:
                entries.append(record)

            # Save the batch when we have a suitable number of records.
            if len(entries) >= BATCH_SIZE:
                self.save(entries)
                entries = []

        # Save the final batch, if required.
        if len(entries) >= 0:
            self.save(entries)

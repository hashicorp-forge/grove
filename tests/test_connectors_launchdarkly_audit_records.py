# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the LaunchDarkly audit records collector."""

import os
import re
import unittest
from unittest.mock import patch

import responses

from grove.connectors.launchdarkly.audit_records import Connector
from grove.models import ConnectorConfig
from tests import mocks


class LaunchDarklyAuditTestCase(unittest.TestCase):
    """Implements tests for the LaunchDarkly audit records collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="1FEEDFEED1",
                key="token",
                name="test",
                connector="test",
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_collect_rate_limit(self):
        """Ensure rate-limit retires are working as expected."""
        # Rate limit the first request.
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog\?"),
            status=429,
            content_type="application/json",
            body=bytes(),
        )

        # Succeed on the second.
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog\?"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_list_1.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Include a generic secondary item query
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/[a-z0-9-]+"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_1.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        

        # Ensure time.sleep is called with the correct value in response to a
        # rate-limit.
        with patch("time.sleep", return_value=None) as mock_sleep:
            self.connector.run()
            mock_sleep.assert_called_with(1)

    @responses.activate
    def test_collect_pagination(self):
        """Ensure pagination is working as expected."""
        # Succeed with a cursor returned (to indicate paging is required).
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog\?after=[0-9]+&limit=[0-9]+"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_list_2.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # The last "page" returns an empty cursor.
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog\?after=[0-9]+&before=[0-9]+&limit=[0-9]+"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_list_3.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Include item queries
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/entry-def456"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_1.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/entry-jkl789"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_2.json"),
                    "r",
                ).read(),
                "utf-8",
                ),
        )
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/entry-stu012"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_3.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/abcd1111"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_4.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/abcd2222"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_5.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/abcd3333"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_6.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Check the pointer matches the latest execution_time value, and that the
        # expected number of logs were returned.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 6)
        self.assertEqual(self.connector.pointer, "1755692329415")

    @responses.activate
    def test_collect_no_pagination(self):
        """Ensure collection without pagination is working as expected."""
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog\?"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_list_3.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/abcd1111"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_4.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/abcd2222"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_5.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )
        responses.add(
            responses.GET,
            re.compile(r"https://app.launchdarkly.com/api/v2/auditlog/abcd3333"),
            status=200,
            content_type="application/json",
            body=bytes(
                open(
                    os.path.join(self.dir, "fixtures/launchdarkly/audit_log_item_6.json"),
                    "r",
                ).read(),
                "utf-8",
            ),
        )

        # Set the chunk size large enough that no chunking is required.
        self.connector.run()
        self.assertEqual(self.connector._saved["logs"], 3)
        self.assertEqual(self.connector.pointer, "1755689827802")

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements unit tests for the Okta Users collector."""

import os
import unittest
from unittest.mock import patch

import responses

from grove.connectors.okta.users import Connector
from grove.models import ConnectorConfig
from tests import mocks


class OktaUsersTestCase(unittest.TestCase):
    """Implements unit tests for the Okta Users collector."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def setUp(self):
        """Ensure the application is setup for testing."""
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.connector = Connector(
            config=ConnectorConfig(
                identity="test-org",
                key="token",
                name="test",
                connector="test",
                disabled=False,
                secrets={},
                encoding={},
                operation="all",
                frequency=0,
                processors=[],
                outputs={},
            ),
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )

    @responses.activate
    def test_collect_pagination(self):
        """Ensure pagination is working as expected."""
        # Mock the first page of users
        responses.add(
            responses.GET,
            "https://test-org.okta.com/api/v1/users?limit=1000",
            json=[
                {
                    "id": "00u1abc123def456",
                    "status": "ACTIVE",
                    "created": "2023-01-15T10:30:00.000Z",
                    "activated": "2023-01-15T10:30:00.000Z",
                    "statusChanged": "2023-01-15T10:30:00.000Z",
                    "lastLogin": "2024-01-15T14:22:00.000Z",
                    "lastUpdated": "2024-01-15T14:22:00.000Z",
                    "passwordChanged": "2023-01-15T10:30:00.000Z",
                    "profile": {
                        "firstName": "Test",
                        "lastName": "User",
                        "email": "test.user@fakecompany.com",
                        "login": "test.user@fakecompany.com",
                        "mobilePhone": "+1-555-TEST-123",
                        "secondEmail": "test.personal@fakecompany.com",
                        "title": "Software Engineer",
                        "department": "Engineering",
                        "costCenter": "ENG-001",
                        "manager": "fake.manager@fakecompany.com",
                        "managerId": "00u2def789ghi012",
                    },
                    "credentials": {
                        "password": {},
                        "provider": {
                            "type": "OKTA",
                            "name": "OKTA",
                        },
                    },
                    "_links": {
                        "self": {
                            "href": "https://test-org.okta.com/api/v1/users/00u1abc123def456"
                        }
                    }
                },
                {
                    "id": "00u2def789ghi012",
                    "status": "ACTIVE",
                    "created": "2023-02-20T09:15:00.000Z",
                    "activated": "2023-02-20T09:15:00.000Z",
                    "statusChanged": "2023-02-20T09:15:00.000Z",
                    "lastLogin": "2024-01-14T16:45:00.000Z",
                    "lastUpdated": "2024-01-14T16:45:00.000Z",
                    "passwordChanged": "2023-02-20T09:15:00.000Z",
                    "profile": {
                        "firstName": "Fake",
                        "lastName": "Manager",
                        "email": "fake.manager@fakecompany.com",
                        "login": "fake.manager@fakecompany.com",
                        "mobilePhone": "+1-555-FAKE-456",
                        "title": "Engineering Manager",
                        "department": "Engineering",
                        "costCenter": "ENG-002",
                    },
                    "credentials": {
                        "password": {},
                        "provider": {
                            "type": "OKTA",
                            "name": "OKTA",
                        },
                    },
                    "_links": {
                        "self": {
                            "href": "https://test-org.okta.com/api/v1/users/00u2def789ghi012"
                        }
                    }
                }
            ],
            headers={
                "Link": '<https://test-org.okta.com/api/v1/users?limit=1000&after=00u2def789ghi012>; rel="next", <https://test-org.okta.com/api/v1/users?limit=1000>; rel="self"'
            },
            status=200,
        )

        # Mock the second page of users
        responses.add(
            responses.GET,
            "https://test-org.okta.com/api/v1/users?limit=1000&after=00u2def789ghi012",
            json=[
                {
                    "id": "00u3ghi345jkl678",
                    "status": "SUSPENDED",
                    "created": "2023-03-10T14:20:00.000Z",
                    "activated": "2023-03-10T14:20:00.000Z",
                    "statusChanged": "2023-12-01T08:30:00.000Z",
                    "lastLogin": "2023-11-30T17:15:00.000Z",
                    "lastUpdated": "2023-12-01T08:30:00.000Z",
                    "passwordChanged": "2023-03-10T14:20:00.000Z",
                    "profile": {
                        "firstName": "Dummy",
                        "lastName": "Data",
                        "email": "dummy.data@fakecompany.com",
                        "login": "dummy.data@fakecompany.com",
                        "mobilePhone": "+1-555-DUMMY-789",
                        "title": "Product Manager",
                        "department": "Product",
                        "costCenter": "PROD-001",
                        "manager": "mock.manager@fakecompany.com",
                        "managerId": "00u4jkl901mno234",
                    },
                    "credentials": {
                        "password": {},
                        "provider": {
                            "type": "OKTA",
                            "name": "OKTA",
                        },
                    },
                    "_links": {
                        "self": {
                            "href": "https://test-org.okta.com/api/v1/users/00u3ghi345jkl678"
                        }
                    }
                }
            ],
            headers={
                "Link": '<https://test-org.okta.com/api/v1/users?limit=1000>; rel="self"'
            },
            status=200,
        )

        # Execute the connector
        self.connector.collect()

        # Verify we made the expected API calls
        self.assertEqual(len(responses.calls), 2)
        self.assertEqual(
            responses.calls[0].request.url,
            "https://test-org.okta.com/api/v1/users?limit=1000"
        )
        self.assertEqual(
            responses.calls[1].request.url,
            "https://test-org.okta.com/api/v1/users?limit=1000&after=00u2def789ghi012"
        )

    @responses.activate
    def test_collect_single_page(self):
        """Ensure single page collection works as expected."""
        # Mock a single page response
        responses.add(
            responses.GET,
            "https://test-org.okta.com/api/v1/users?limit=1000",
            json=[
                {
                    "id": "00u1abc123def456",
                    "status": "ACTIVE",
                    "created": "2023-01-15T10:30:00.000Z",
                    "activated": "2023-01-15T10:30:00.000Z",
                    "statusChanged": "2023-01-15T10:30:00.000Z",
                    "lastLogin": "2024-01-15T14:22:00.000Z",
                    "lastUpdated": "2024-01-15T14:22:00.000Z",
                    "passwordChanged": "2023-01-15T10:30:00.000Z",
                    "profile": {
                        "firstName": "Mock",
                        "lastName": "Director",
                        "email": "mock.director@fakecompany.com",
                        "login": "mock.director@fakecompany.com",
                        "mobilePhone": "+1-555-MOCK-001",
                        "title": "Director of Engineering",
                        "department": "Engineering",
                        "costCenter": "ENG-003",
                    },
                    "credentials": {
                        "password": {},
                        "provider": {
                            "type": "OKTA",
                            "name": "OKTA",
                        },
                    },
                    "_links": {
                        "self": {
                            "href": "https://test-org.okta.com/api/v1/users/00u1abc123def456"
                        }
                    }
                }
            ],
            headers={
                "Link": '<https://test-org.okta.com/api/v1/users?limit=1000>; rel="self"'
            },
            status=200,
        )

        # Execute the connector
        self.connector.collect()

        # Verify we made the expected API call
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(
            responses.calls[0].request.url,
            "https://test-org.okta.com/api/v1/users?limit=1000"
        )

    @responses.activate
    def test_collect_empty_response(self):
        """Ensure empty response is handled gracefully."""
        # Mock an empty response
        responses.add(
            responses.GET,
            "https://test-org.okta.com/api/v1/users?limit=1000",
            json=[],
            headers={
                "Link": '<https://test-org.okta.com/api/v1/users?limit=1000>; rel="self"'
            },
            status=200,
        )

        # Execute the connector
        self.connector.collect()

        # Verify we made the expected API call
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(
            responses.calls[0].request.url,
            "https://test-org.okta.com/api/v1/users?limit=1000"
        )

    @responses.activate
    def test_collect_rate_limit(self):
        """Ensure rate-limit retries are working as expected."""
        # Mock rate limit response
        responses.add(
            responses.GET,
            "https://test-org.okta.com/api/v1/users?limit=1000",
            json={"errorCode": "E0000047", "errorSummary": "Rate limit exceeded"},
            headers={
                "X-Rate-Limit-Remaining": "0",
                "X-Rate-Limit-Reset": "1640995200",
            },
            status=429,
        )

        # Mock successful response after rate limit
        responses.add(
            responses.GET,
            "https://test-org.okta.com/api/v1/users?limit=1000",
            json=[
                {
                    "id": "00u1abc123def456",
                    "status": "ACTIVE",
                    "created": "2023-01-15T10:30:00.000Z",
                    "activated": "2023-01-15T10:30:00.000Z",
                    "statusChanged": "2023-01-15T10:30:00.000Z",
                    "lastLogin": "2024-01-15T14:22:00.000Z",
                    "lastUpdated": "2024-01-15T14:22:00.000Z",
                    "passwordChanged": "2023-01-15T10:30:00.000Z",
                    "profile": {
                        "firstName": "Sample",
                        "lastName": "Person",
                        "email": "sample.person@fakecompany.com",
                        "login": "sample.person@fakecompany.com",
                    },
                    "credentials": {
                        "password": {},
                        "provider": {
                            "type": "OKTA",
                            "name": "OKTA",
                        },
                    },
                    "_links": {
                        "self": {
                            "href": "https://test-org.okta.com/api/v1/users/00u1abc123def456"
                        }
                    }
                }
            ],
            headers={
                "Link": '<https://test-org.okta.com/api/v1/users?limit=1000>; rel="self"'
            },
            status=200,
        )

        # Execute the connector
        self.connector.collect()

        # Verify we made the expected API calls (rate limit + success)
        self.assertEqual(len(responses.calls), 2)

    def test_domain_property_default(self):
        """Ensure domain property returns default when not configured."""
        self.assertEqual(self.connector.domain, "okta.com")

    def test_domain_property_configured(self):
        """Ensure domain property returns configured value."""
        # Create a new connector with domain configured
        config = ConnectorConfig(
            identity="test-org",
            key="token",
            name="test",
            connector="test",
            disabled=False,
            secrets={},
            encoding={},
            operation="all",
            frequency=0,
            processors=[],
            outputs={},
        )
        config.domain = "okta-emea.com"
        
        connector = Connector(
            config=config,
            context={
                "runtime": "test_harness",
                "runtime_id": "NA",
            },
        )
        
        self.assertEqual(connector.domain, "okta-emea.com")

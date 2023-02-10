"""Implements tests for connector locking."""

import os
import time
import unittest
from unittest.mock import patch

from grove.connectors import BaseConnector
from grove.constants import ENV_GROVE_LOCK_DURATION
from grove.exceptions import ConcurrencyException
from grove.models import ConnectorConfig
from tests import mocks


class ConnectorLockingTestCase(unittest.TestCase):
    """Implements tests for connector locking."""

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_lock_acquire(self):
        """Ensures locks can be acquired and concurrent executions are prevented."""
        config = ConnectorConfig(
            key="token",
            name="test",
            identity="1FEEDFEED1",
            connector="example_one",
            operation="first",
        )
        context = {
            "runtime": "test_harness",
            "runtime_id": "NA",
        }

        # Acquire a lock.
        first_execution = BaseConnector(config=config, context=context)
        first_execution.lock()

        # Simulate a parallel execution of the same connector instance.
        second_execution = BaseConnector(config=config, context=context)

        # In-memory cache does not support locking as it's only intended for local
        # "one-shot" execution, and development use. As a result, we have to be naughty
        # and clone the state of one cache to the other to simulate this.
        second_execution._cache._data = first_execution._cache._data

        # Ensure acquiring the lock fails.
        with self.assertRaises(ConcurrencyException):
            second_execution.lock()

    @patch("grove.helpers.plugin.load_handler", mocks.load_handler)
    def test_lock_takeover(self):
        """Ensures locks can be taken over if too old."""
        # Override lock duration to something trivial.
        os.environ[ENV_GROVE_LOCK_DURATION] = str("5")

        config = ConnectorConfig(
            key="token",
            name="test",
            identity="1FEEDFEED1",
            connector="example_one",
            operation="first",
        )
        context = {
            "runtime": "test_harness",
            "runtime_id": "NA",
        }

        # Acquire a lock.
        first_execution = BaseConnector(config=config, context=context)
        first_execution.lock()

        # Attempt to acquire a lock for the same connector instance - after the lock
        # has expired. This should succeed due to expiry.
        second_execution = BaseConnector(config=config, context=context)

        # In-memory cache does not support locking as it's only intended for local
        # "one-shot" execution, and development use. As a result, we have to be naughty
        # and clone the state of one cache to the other to simulate this.
        second_execution._cache._data = first_execution._cache._data

        # Ensure acquiring the lock fails before expiration.
        with self.assertRaises(ConcurrencyException):
            second_execution.lock()

        # Wait to ensure the lock has expired.
        time.sleep(5)

        # Ensure the lock is taken-over successfully after expiration.
        second_execution.lock()

        # Ensure subsequent attempts of the first fail due to this lock takeover.
        with self.assertRaises(ConcurrencyException):
            first_execution.lock()

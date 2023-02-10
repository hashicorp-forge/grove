"""Implements an example set of tests."""

import unittest


class ExampleTestCase(unittest.TestCase):
    """Implements an example set of tests."""

    def setUp(self):
        """Ensure the application is setup for testing."""
        pass

    def tearDown(self):
        """Ensure everything is torn down between tests."""
        pass

    def test_example(self):
        """Always passes."""
        self.assertEqual(True, True)

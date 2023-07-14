# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for parsing helpers."""

import unittest

from grove.helpers import parsing


class ParsingHelpersTestCase(unittest.TestCase):
    """Implements tests for parsing helpers."""

    def test_update_by_path(self):
        """Ensures path updating operates as expected."""
        # Multi-dimension.
        expected_multi = {
            "A": {
                "B": {
                    "C": {
                        "D": {
                            "E": "injected",
                            "deepest": True,
                        },
                        "adjacent": True,
                    }
                }
            },
            "top": True,
        }
        self.assertDictEqual(
            expected_multi,
            parsing.update_path(
                {
                    "A": {"B": {"C": {"D": {"deepest": True}, "adjacent": True}}},
                    "top": True,
                },
                "A.B.C.D.E".split("."),
                "injected",
            ),
        )

        # Replacement.
        expected_replace = {
            "A": {"B": {"C": "replaced"}},
        }
        self.assertDictEqual(
            expected_replace,
            parsing.update_path(
                {"A": {"B": {"C": "initial"}}},
                "A.B.C".split("."),
                "replaced",
            ),
        )

        # Single dimension.
        expected_single = {
            "A": "value",
        }
        self.assertDictEqual(
            expected_single,
            parsing.update_path(
                {},
                "A".split("."),
                "value",
            ),
        )

        # Deletion of nested keys.
        self.assertDictEqual(
            {"A": 1},
            parsing.update_path(
                {"A": 1, "B": {"C": [1, 2, 3], "D": {"E": "F"}}},
                "B".split("."),
                None,
            ),
        )

        # Deletion of deeply nested keys.
        self.assertDictEqual(
            {"A": 1, "B": {"D": {"E": "F"}}},
            parsing.update_path(
                {"A": 1, "B": {"C": [1, 2, 3], "D": {"E": "F"}}},
                "B.C".split("."),
                None,
            ),
        )

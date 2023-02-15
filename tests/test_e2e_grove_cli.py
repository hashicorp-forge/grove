# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements e2e tests for grove using the heartbeat connector."""

import json
import os
import subprocess
import unittest


class GroveCLITestCase(unittest.TestCase):
    """Implements e2e tests for grove."""

    def test_grove_cli(self):
        """Ensure messages are created and printed to stdout."""
        connectors_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "../templates/deployment/local-quick-start"
            )
        )

        env = os.environ | {
            "GROVE_OUTPUT_HANDLER": "local_stdout",
            "GROVE_CONFIG_HANDLER": "local_file",
            "GROVE_CACHE_HANDLER": "local_memory",
            "GROVE_CONFIG_LOCAL_FILE_PATH": connectors_path,
        }

        out = subprocess.run(["grove"], capture_output=True, env=env)
        self.assertEqual(out.returncode, 0)

        for logline in out.stderr.split(b"\n"):
            if logline:
                self.assertIn(json.loads(logline)["level"], ["INFO", "DEBUG"])

        parsed_loglines = []
        for logline in out.stdout.split(b"\n"):
            if logline:
                parsed_loglines.append(json.loads(logline))

        self.assertEqual(len(parsed_loglines), 5)
        self.assertEqual(parsed_loglines[0]["message"]["type"], "heartbeat")

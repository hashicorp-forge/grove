# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the local file secrets backend."""

import os
import tempfile
import unittest

from grove.secrets.local_file import Handler


class SecretsLocalFileTestCase(unittest.TestCase):
    """Implements tests for the local file secrets backend."""

    def setUp(self):
        self.fixtures = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "fixtures/")
        )

    def test_relative_path(self):
        """Ensures a secret can be read from a relative file path."""
        expected = "_Super_S3cret_Stuff."

        with tempfile.NamedTemporaryFile("w") as fout:
            fout.write(expected)
            fout.write("\n")
            fout.flush()

            # Validate loading with a path prefix.
            os.environ["GROVE_SECRET_LOCAL_FILE_PATH_PREFIX"] = os.path.dirname(
                fout.name
            )

            self.secrets = Handler()
            self.assertEqual(self.secrets.get(os.path.basename(fout.name)), expected)

    def test_absolute_path(self):
        """Ensures a secret can be read from an absolute file path."""
        expected = "_Super_S3cret_Stuff."

        with tempfile.NamedTemporaryFile("w") as fout:
            fout.write(expected)
            fout.write("\n")
            fout.flush()

            # Validate loading with a path prefix.
            self.secrets = Handler()
            self.assertEqual(self.secrets.get(fout.name), expected)

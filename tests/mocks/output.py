# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Mocks a Grove output provider."""

from grove.outputs import BaseOutput


class TestHandler(BaseOutput):
    def submit(self, *arg, **kwargs):
        """Does nothing, successfully."""
        return

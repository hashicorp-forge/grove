# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides Mock implementations for unit and integration tests."""

from typing import Any

from grove.caches import local_memory
from grove.constants import PLUGIN_GROUP_CACHE, PLUGIN_GROUP_OUTPUT
from grove.helpers import plugin
from tests.mocks import output  # noqa: F401


def load_handler(name: str, group: str, *args, **kwargs) -> Any:
    """Wraps handler loading to load predefined mocks for a given group."""
    if group == PLUGIN_GROUP_OUTPUT:
        return output.TestHandler()

    if group == PLUGIN_GROUP_CACHE:
        return local_memory.Handler()

    cls = plugin.lookup_handler(name, group).load()
    return cls(*args, **kwargs)

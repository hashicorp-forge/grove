# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides helpers for plugin loading."""

from importlib.metadata import EntryPoint, entry_points
from typing import Any

from grove.exceptions import ConfigurationException


def lookup_handler(name: str, group: str) -> EntryPoint:
    """Attempts to locate requested plugin handler.

    This utilises setuptools entrypoints to allow handlers to register themselves for
    use by Grove.

    :param name: The name of the handler to load (e.g. 'aws_ssm').
    :param group: The group the handler belongs to (e.g. 'grove.outputs').

    :raises ConfigurationException: The specified handler could not be located.
    """
    # The 'group' kwarg for entry_points was added in Python 3.10, in order to support
    # older versions of Python 3 we will not be using this feature. Additionally, using
    # get() will raise deprecation warnings to use select() instead. However, select()
    # was also added in Python 3.10, which would break backwards compatibility...
    eps = entry_points()

    for candidate in eps.get(group, []):
        if candidate.name == name:
            return candidate

    raise ConfigurationException(
        f"Requested handler could not be found with name '{name}' (group '{group}')"
    )


def load_handler(name: str, group: str, *args: Any, **kwargs: Any) -> Any:
    """Attempts to locate and load the requested plugin handler.

    This is a convenience method which wrappers the lookup operation, and performs the
    load and instantiation required to hydrate a 'real' handler. Any additional
    arguments passed to load_handler are pass through to the handler during creation.

    :param name: The name of the handler to load (e.g. 'aws_ssm').
    :param group: The group the handler belongs to (e.g. 'grove.outputs').
    """
    cls = lookup_handler(name, group).load()

    return cls(*args, **kwargs)

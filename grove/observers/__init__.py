# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides Grove observability events to supported destinations.

Observers form the basis of Grove's observability layer. Unlike output handlers - which
persist collected log data - observers emit small, discrete control-plane events which
describe the health of a connector (such as errors, stale collections, or runs which
returned no data).

Observers are intentionally decoupled from collection: a failing or slow observer must
never affect a collection. As a result all emission should be performed via
:func:`emit`, which swallows and logs any exceptions raised by a handler.
"""

import abc
import logging
import os

from pydantic import BaseSettings, Extra, ValidationError

from grove.constants import ENV_GROVE_OBSERVER_HANDLER, PLUGIN_GROUP_OBSERVER
from grove.exceptions import ConfigurationException
from grove.helpers import parsing, plugin
from grove.models import ObserverEvent, ObserverEventType


class BaseObserver(abc.ABC):
    """The basis for all Grove observer handlers."""

    class Configuration(BaseSettings, extra=Extra.allow):
        """Defines the configuration directives required by all observer handlers."""

        # An optional comma-delimited allow-list of event types to emit. If unset, all
        # event types are emitted.
        events: str | None = None

    def __init__(self):
        """Implements core logic which applies to all handlers.

        This includes configuration of logging, and parsing of configuration.
        """
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = self.Configuration()
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

    def setup(self):
        """Implements logic to setup any required clients, sockets, or connections.

        If not required for the given observer handler, this may be a no-op.
        """

    def enabled_for(self, event: ObserverEventType) -> bool:
        """Determines whether this observer should emit the given event type.

        :param event: The event type to check.

        :return: True if the event should be emitted, False if it should be filtered.
        """
        if not self.config.events:
            return True

        allowed = {candidate.strip() for candidate in self.config.events.split(",")}

        return event.value in allowed

    @abc.abstractmethod
    def emit(self, event: ObserverEvent):
        """Implements logic required to emit an observability event to a destination.

        Handlers should raise on failure to allow the caller to log the failure. The
        caller is responsible for ensuring that failures do not affect collection.

        :param event: The observability event to emit.
        """


def load() -> BaseObserver | None:
    """Loads and configures the observer handler, if one is configured.

    The observer handler is selected via the ``GROVE_OBSERVER_HANDLER`` environment
    variable. If unset, observability is disabled and None is returned.

    :raises ConfigurationException: The configured handler could not be loaded.

    :return: A configured observer handler, or None if none is configured.
    """
    handler = os.environ.get(ENV_GROVE_OBSERVER_HANDLER)
    if not handler:
        return None

    observer = plugin.load_handler(handler, PLUGIN_GROUP_OBSERVER)
    observer.setup()

    return observer


def emit(observer: BaseObserver | None, event: ObserverEvent):
    """Safely emits an event via the provided observer, if one is configured.

    This helper ensures that a missing, filtered, or failing observer never affects the
    caller. Any exception raised by the handler is logged and swallowed.

    :param observer: The observer handler to emit via, or None to skip emission.
    :param event: The observability event to emit.
    """
    if observer is None:
        return

    if not observer.enabled_for(event.event):
        return

    # This exception handler is intentionally broad. An observability layer must never
    # be able to break, slow, or otherwise interfere with collection.
    try:
        observer.emit(event)
    except Exception as err:
        logging.getLogger(__name__).warning(
            "Observer failed to emit event, continuing.",
            extra={
                "exception": err,
                "event": event.event.value,
                "connector": event.connector,
                "identity": event.identity,
                "operation": event.operation,
            },
        )

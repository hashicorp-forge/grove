# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Mocks a Grove observer handler."""

from typing import List

from grove.models import ObserverEvent
from grove.observers import BaseObserver


class TestHandler(BaseObserver):
    """A recording observer used to assert emitted events during tests."""

    __test__ = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.events: List[ObserverEvent] = []

    def emit(self, event: ObserverEvent):
        """Records the emitted event."""
        self.events.append(event)


class RaisingHandler(BaseObserver):
    """An observer which always fails, used to assert failures are swallowed."""

    def emit(self, event: ObserverEvent):
        """Always raises."""
        raise RuntimeError("This observer always fails.")

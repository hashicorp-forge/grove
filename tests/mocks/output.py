"""Mocks a Grove output provider."""

from grove.outputs import BaseOutput


class TestHandler(BaseOutput):
    def submit(self, *arg, **kwargs):
        """Does nothing, successfully."""
        return

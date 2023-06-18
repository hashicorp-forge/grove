"""{{ cookiecutter.provider_name }} {{ cookiecutter.provider_product }} example logs connector for Grove."""

from datetime import datetime, timedelta, timezone

from grove.connectors import BaseConnector
from grove.constants import CHRONOLOGICAL
from grove.exceptions import NotFoundException
from grove.types import AuditLogEntries


class Connector(BaseConnector):
    NAME = "{{ cookiecutter.provider_name }}_{{ cookiecutter.provider_product }}_example_logs"
    LOG_ORDER = CHRONOLOGICAL
    POINTER_PATH = "timestamp"

    @property
    def optional_setting(self):
        """Fetches an optional setting from the connector configuration document.

        :return: The "optional_setting" component of the connector configuration.
        """
        try:
            return self.configuration.optional_setting
        except AttributeError:
            return "Some Default value"

    def collect(self):
        """Collects example logs from the {{ cookiecutter.provider_name }} API.

        This will first check whether there are any pointers cached to indicate previous
        collections. If not, the last week of data will be collected.
        """
        # TODO: Setup API client for product.

        try:
            _ = self.pointer
        except NotFoundException:
            # TODO: Change the default pointer value to the correct format.
            self.pointer = str(
                int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())
            )

        # TODO: Retrieve logs from API in pages in a loop, calling self.save() on each
        # page to reduce memory use, and allow partial collection.
        while True:
            # TODO: Replace with call to API.
            log = AuditLogEntries(None, [])

            # Save this batch of log entries.
            self.save(log.entries)

            # Break out of loop when there are no more pages.
            cursor = log.cursor  # type: ignore
            if cursor is None:
                break

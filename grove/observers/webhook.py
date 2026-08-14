# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove webhook observer handler."""


import requests
from pydantic import Field

from grove.exceptions import AccessException
from grove.models import ObserverEvent
from grove.observers import BaseObserver


class Handler(BaseObserver):
    """This observer handler emits events to an HTTP(S) webhook as JSON."""

    class Configuration(BaseObserver.Configuration):
        """Defines environment variables used to configure the webhook handler.

        This should also include any appropriate default values for fields which are not
        required.
        """

        url: str = Field(
            description="The fully-qualified URL to POST observability events to.",
        )
        headers: str | None = Field(
            description="A pipe delimited set of HTTP headers to add ('key: value').",
            default=None,
        )
        timeout: int = Field(
            description="The maximum time to wait before a request times out (seconds)",
            default=10,
        )
        retries: int = Field(
            description="The maximum number of retries before failing to emit.",
            default=3,
        )
        insecure: bool = Field(
            description="Whether to accept invalid certificates for HTTPS endpoints.",
            default=False,
        )

        class Config:
            """Allow environment variable override of configuration fields.

            This also enforces a prefix for all environment variables for this handler.
            As an example the field `url` would be set using the environment variable
            `GROVE_OBSERVER_WEBHOOK_URL`.
            """

            env_prefix = "GROVE_OBSERVER_WEBHOOK_"
            case_insensitive = True

    def setup(self):
        """Parses and sets up HTTP headers.

        This method parses pipe delimited HTTP headers from the environment. This is not
        perfect, but we're relatively limited when using environment variables while
        wishing to retain compatibility across runtimes.
        """
        self._headers = {
            "Content-Type": "application/json",
        }

        # Construct the headers from the configured pipe delimited values.
        if self.config.headers:
            for header in self.config.headers.split("|"):
                key = header.split(":")[0]
                value = ":".join(header.split(":")[1:]).lstrip(" ")

                # Downcase for comparison to ensure we don't add duplicates.
                for existing in self._headers.copy().keys():
                    # Delete the old value entirely if there is one present.
                    if existing.lower() == key.lower():
                        del self._headers[existing]

                # Add the new value.
                self._headers[key] = value

    def emit(self, event: ObserverEvent):
        """Performs an HTTP POST with the body containing the event as JSON.

        :param event: The observability event to emit.

        :raises AccessException: The event could not be emitted after all retries.
        """
        attempts = 0

        # Whether we need to verify certificates. We define this here to avoid negating
        # booleans or using ternaries later on which may lead to confusion later.
        verify = True

        if self.config.insecure == True:  # noqa: E712
            verify = False

        # Only attempt to POST the configured number of times, otherwise bail.
        while attempts < self.config.retries:
            try:
                response = requests.post(
                    self.config.url,
                    data=event.json(),
                    headers=self._headers,
                    timeout=self.config.timeout,
                    verify=verify,
                )

                # Break out of the retry loop on success, otherwise loop and retry.
                response.raise_for_status()
                return
            except requests.exceptions.RequestException as err:
                attempts += 1

                if attempts >= self.config.retries:
                    raise AccessException(
                        f"Unable to emit observability event to webhook: {err}"
                    )

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove remote HTTP output handler."""

import json
from typing import Any, Dict, List, Optional

import requests
from pydantic import Field

from grove.constants import GROVE_METADATA_KEY
from grove.exceptions import AccessException, DataFormatException
from grove.outputs import BaseOutput


class Handler(BaseOutput):
    class Configuration(BaseOutput.Configuration):
        """Defines environment variables used to configure the remote HTTP handler.

        This should also include any appropriate default values for fields which are not
        required.
        """

        url: str = Field(
            description="The fully-qualified URL to POST logs to",
        )
        retries: int = Field(
            description="The maximum number of retries before failing the collection.",
            default=5,
        )
        headers: Optional[str] = Field(
            description="A pipe delimited set of HTTP headers to add ('key: value').",
            default=None,
        )
        timeout: int = Field(
            description="The maximum time to wait before a request times out (seconds)",
            default=10,
        )
        insecure: bool = Field(
            description="Whether to accept invalid certificates for HTTPS endpoints.",
            default=False,
        )

        class Config:
            """Allow environment variable override of configuration fields.

            This also enforce a prefix for all environment variables for this handler.
            As an example the field `url` would be set using the environment variable
            `GROVE_OUTPUT_REMOTE_HTTP_URL`.
            """

            env_prefix = "GROVE_OUTPUT_REMOTE_HTTP_"
            case_insensitive = True

    def setup(self):
        """Parses and sets up HTTP headers.

        This method parses pipe delimited HTTP headers from the environment. This is not
        perfect, but we're relatively limited when using environment variables while
        wishing to retain compatibility across runtimes.
        """
        # The content-type can be updated by the caller, if they wish.
        self._headers = {
            "Content-Type": "application/x-ndjson",
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

    def submit(
        self,
        data: bytes,
        connector: str,
        identity: str,
        operation: str,
        part: int = 0,
        kind: Optional[str] = None,
        descriptor: Optional[str] = None,
    ):
        """Performs an HTTP POST with the body containing collected logs as NDJSON.

        :param data: Log data to POST.
        :param connector: Name of the connector which retrieved the data.
        :param identity: Identity the collected data was collect for.
        :param operation: Operation the collected logs are associated with.
        :param part: Number indicating which part of the same log stream this file
            contains data for.
        :param kind: Currently not used by this output plugin.
        :param descriptor: Currently not used by this output plugin.

        :raises AccessException: An issue occurred when writing data.
        """
        attempts = 0

        # Whether we need to verify certificates. We define this here to avoid negating
        # booleans or using ternaries later on which may lead to confusion later.
        verify = True

        if self.config.insecure == True:  # noqa: E712
            verify = False

        # Only attempt to POST the configured number of times, otherwise bail and allow
        # retry on next collection.
        while attempts < self.config.retries:
            try:
                response = requests.post(
                    self.config.url,
                    data=data,
                    headers=self._headers,
                    timeout=self.config.timeout,
                    verify=verify,
                )

                # Break out of the retry look on success, otherwise loop and retry.
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as err:
                attempts += 1

                if attempts >= self.config.retries:
                    raise AccessException(
                        f"Unable to submit log data to HTTP endpoint: {err}"
                    )

    def serialize(self, data: List[Any], metadata: Dict[str, Any] = {}) -> bytes:
        """Implements serialization of log entries to NDJSON.

        :param data: A list of log entries to serialize to JSON.
        :param metadata: Metadata to append to each log entry before serialization. If
            not specified no metadata will be added.

        :return: Log data serialized as NDJSON (as bytes).

        :raises DataFormatException: Cannot serialize the input to JSON.
        """
        candidate = []

        # Append the Grove metadata to each log entry, and serialize to JSON.
        for entry in data:
            # Skip empty log entries.
            if entry is None:
                continue

            if metadata:
                entry[GROVE_METADATA_KEY] = {
                    **metadata,
                    **entry.get(GROVE_METADATA_KEY, {}),
                }

            # We don't want to silently drop and lose single records, so drop the entire
            # batch if there is bad data (which will trigger a retry next run).
            try:
                candidate.append(json.dumps(entry, separators=(",", ":")))
            except TypeError as err:
                raise DataFormatException(f"Unable to serialize to JSON: {err}")

        return bytes("\r\n".join(candidate), "utf-8")

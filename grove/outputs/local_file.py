# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove local file path output handler."""

import datetime
import os
from typing import Optional

from pydantic import Field

from grove.constants import DATESTAMP_FORMAT
from grove.exceptions import AccessException
from grove.outputs import BaseOutput

OBJECT_PATH = (
    "{descriptor}{connector}/{identity}/{year}/{month}/{day}/"
    "{operation}-{datestamp}.{part}{kind}"
)


class Handler(BaseOutput):
    class Configuration(BaseOutput.Configuration):
        """Defines environment variables used to configure the local file handler.

        This should also include any appropriate default values for fields which are not
        required.
        """

        path: str = Field(
            description="The path to the directory to write collected logs to.",
        )

        class Config:
            """Allow environment variable override of configuration fields.

            This also enforce a prefix for all environment variables for this handler.
            As an example the field `path` would be set using the environment variable
            `GROVE_OUTPUT_LOCAL_FILE_PATH`.
            """

            env_prefix = "GROVE_OUTPUT_LOCAL_FILE_"
            case_insensitive = True

    def setup(self):
        """Set up access to local filesystem path.

        This also checks that an output directory is configured, and it is initially
        accessible and writable.

        :raises AccessException: There was an issue accessing to the provided file path.
        """
        # Perform a spot check to see if the directory is writable now. Although this
        # can change, we'd like to bail before we collect any data if it's a simple
        # permissions related misconfiguration.
        if not os.path.isdir(self.config.path):
            raise AccessException(
                f"Configured output path '{self.config.path}' does not exist."
            )

        if not os.access(self.config.path, os.W_OK | os.X_OK):
            raise AccessException(
                f"Configured output path '{self.config.path}' is not writable."
            )

    def submit(
        self,
        data: bytes,
        connector: str,
        identity: str,
        operation: str,
        part: int = 0,
        kind: Optional[str] = ".json.gz",
        descriptor: Optional[str] = "logs/",
    ):
        """Persists captured data to a local file path.

        :param data: Log data to write.
        :param connector: Name of the connector which retrieved the data.
        :param identity: Identity the collected data was collect for.
        :param operation: Operation the collected logs are associated with.
        :param part: Number indicating which part of the same log stream this file
            contains data for. This is used to indicate that the logs are from the same
            collection, but have been broken into smaller files for downstream
            processing.
        :param kind: An optional file suffix to use for files written.
        :param descriptor: An optional path to append to the beginning of the output
            file path.

        :raises AccessException: An issue occurred when writing data.
        """
        # Append a trailing slash to the descriptor if set - to form a path.
        if descriptor and not descriptor.endswith("/"):
            descriptor = f"{descriptor}/"

        # Each log file is output under a particular hierarchy to assist with sharding
        # and ingestion / finding of log data.
        datestamp = datetime.datetime.utcnow()

        filename = OBJECT_PATH.format(
            part=part,
            connector=connector,
            identity=identity,
            operation=operation,
            year=datestamp.strftime("%Y"),
            month=datestamp.strftime("%m"),
            day=datestamp.strftime("%d"),
            datestamp=datestamp.strftime(DATESTAMP_FORMAT),
            kind=kind,
            descriptor=descriptor,
        )

        # Quick and dirty directory traversal check.
        destination = os.path.abspath(
            os.path.join(self.config.path, os.path.dirname(filename))
        )

        if not destination.startswith(self.config.path):
            raise AccessException(
                f"Generated output filepath '{destination}' is outside of the "
                f"configured output directory '{self.config.path}'."
            )

        # Create the directory structure, if needed, and write the data.
        try:
            os.makedirs(destination, exist_ok=True)

            with open(os.path.join(self.config.path, filename), "wb") as fout:
                fout.write(data)
        except OSError as err:
            raise AccessException(f"Unable to write log data to file: {err}")

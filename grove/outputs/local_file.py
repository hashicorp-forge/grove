# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove local file path output handler."""

import datetime
import logging
import os

from pydantic import BaseSettings, Field, ValidationError

from grove.constants import DATESTAMP_FORMAT
from grove.exceptions import AccessException, ConfigurationException
from grove.helpers import parsing
from grove.outputs import BaseOutput

OBJECT_PATH = (
    "logs/{connector}/{identity}/{year}/{month}/{day}/"
    "{operation}-{datestamp}.{part}.json.gz"
)


class Configuration(BaseSettings):
    """Defines environment variables used to configure the local file handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    path: str = Field(
        description="The path to the directory to write collected logs to.",
    )

    class Config:
        """Allow environment variable override of configuration fields.

        This also enforce a prefix for all environment variables for this handler. As
        an example the field `path` would be set using the environment variable
        `GROVE_OUTPUT_LOCAL_FILE_PATH`.
        """

        env_prefix = "GROVE_OUTPUT_LOCAL_FILE_"
        case_insensitive = True


class Handler(BaseOutput):
    def __init__(self):
        """Set up access to local filesystem path.

        This also checks that an output directory is configured, and it is initially
        accessible and writable.

        :raises ConfigurationException: There was an issue with output configuration.
        :raises AccessException: There was an issue accessing to the specified file path.
        """
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = Configuration()
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

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

        :raises AccessException: An issue occurred when writing data.
        """
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

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove local file configuration handler."""

import glob
import json
import logging
from json import JSONDecodeError
from typing import List

from pydantic import BaseSettings, Field, ValidationError

from grove.configs import BaseConfig
from grove.exceptions import ConfigurationException, DataFormatException
from grove.helpers import parsing
from grove.models import ConnectorConfig


class Configuration(BaseSettings):
    """Defines environment variables used to configure the local file handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    path: str = Field(
        description="The directory path containing connector configuration documents.",
    )

    class Config:
        """Allow environment variable override of configuration fields.

        This also enforce a prefix for all environment variables for this handler. As
        an example the field `path` would be set using the environment variable
        `GROVE_CONFIG_LOCAL_FILE_PATH`.
        """

        env_prefix = "GROVE_CONFIG_LOCAL_FILE_"
        case_insensitive = True


class Handler(BaseConfig):
    """A configuration handler to read configuration documents from local files."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = Configuration()  # type: ignore
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

    def get(self, id: str = "") -> List[ConnectorConfig]:
        """Get and return one or more connector configuration objects from local files.

        :param id: Not used.

        :return: A list of connector configuration documents (JSON) as strings.
        """
        connectors = []

        # Generate a list of documents for later processing.
        for path in glob.glob(f"{self.config.path}/**/*.json", recursive=True):
            with open(path, "r") as f:
                # We don't want a single bad connector configuration document to break
                # collection, so log an error and continue on a bad document.
                try:
                    connectors.append(ConnectorConfig(**json.load(f)))
                except (JSONDecodeError, ValidationError, DataFormatException) as err:
                    self.logger.error(
                        "Unable to load connector configuration",
                        extra={"path": path, "exception": err},
                    )
                    continue

        return connectors

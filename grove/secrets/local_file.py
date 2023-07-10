# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove local file secrets handler."""

import logging
import os

from pydantic import BaseSettings, Field, ValidationError

from grove.exceptions import AccessException, ConfigurationException
from grove.helpers import parsing
from grove.secrets import BaseSecret


class Configuration(BaseSettings):
    """Defines environment variables used to configure the local file handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    path_prefix: str = Field(
        str(),
        description="An optional prefix to append to configured secret paths.",
    )

    class Config:
        """Allow environment variable override of configuration fields.

        This also enforce a prefix for all environment variables for this handler. As
        an example the field `path` would be set using the environment variable
        `GROVE_SECRET_LOCAL_FILE_PATH_PREFIX`.
        """

        env_prefix = "GROVE_SECRET_LOCAL_FILE_"
        case_insensitive = True


class Handler(BaseSecret):
    """A secret handler to read secrets from local files."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = Configuration()  # type: ignore
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

    def get(self, id: str) -> str:
        """Gets and returns an secret from the specified file path.

        If a path prefix is configured this will be appended to the beginning of the
        configured file path. However, if the path of the secret begins with a '/' it
        the path prefix will be ignored - as it will be considered a fully-qualified
        path specification.

        :param id: The file to read the secret from.

        :return: The plain-text secret, read from the specified file.
        """
        secret = str()
        path = os.path.join(self.config.path_prefix, id)

        try:
            with open(path, "rb") as f:
                secret = str(f.read(), "utf-8").rstrip()
        except (ValidationError, OSError) as err:
            raise AccessException(
                f"Unable to read secret from configured '{path}'. {err}"
            )

        return secret

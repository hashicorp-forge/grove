# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides Grove secret storage using supported backends."""

import abc
import logging
from typing import List

from grove.exceptions import AccessException, DataFormatException
from grove.models import ConnectorConfig, decode


class BaseSecret(abc.ABC):
    def __init__(self):
        """Provides the basis for all Grove secret backends."""
        self.logger = logging.getLogger(__name__)

    @abc.abstractmethod
    def get(self, path: str) -> str:
        """Gets the secret with the given identifier from the given backend.

        :param path: The path to the credential to get.
        :return: The decoded plain-text credential for use by connectors.
        """
        pass

    def load(self, configurations: List[ConnectorConfig]) -> List[ConnectorConfig]:
        """Gets secrets from the backend, inserting them into configuration objects.

        This method should not be implemented by secrets handlers, as the operations
        should be identical between implementations (calls to get()).

        :param configurations: A list of ConnectorConfig objects from the configuration
            backend.

        :return: A list of ConnectorConfig objects with secrets included.
        """
        ready = []

        for configuration in configurations:
            # Fetch the the real secret from the backend using the identifier from the
            # 'secrets' object - decoding it if required.
            try:
                for field, identifier in configuration.secrets.items():
                    self.logger.debug(
                        "Attempting to get query secret from backend",
                        extra={
                            "field": field,
                            "identifier": identifier,
                            "document": configuration.name,
                        },
                    )
                    candidate = self.get(identifier)

                    # Decode the value, if required.
                    if field in configuration.encoding:
                        candidate = decode(candidate, configuration.encoding[field])

                    setattr(configuration, field, candidate)
            except DataFormatException as err:
                self.logger.error(
                    "Unable to decode secret for connector, skipping",
                    extra={
                        "document": configuration.name,
                        "field": field,
                        "exception": err,
                    },
                )
                continue
            except (AccessException, IndexError) as err:
                self.logger.error(
                    "Unable to get secret for connector, skipping",
                    extra={
                        "document": configuration.name,
                        "field": field,
                        "exception": err,
                    },
                )
                continue

            # Some connectors may not have any secrets.
            ready.append(configuration)

        return ready

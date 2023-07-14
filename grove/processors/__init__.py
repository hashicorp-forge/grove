# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides processors for collected log entries."""

import abc
import logging
from typing import Any, Dict, List

from pydantic import Extra, ValidationError

from grove.exceptions import ConfigurationException
from grove.helpers import parsing
from grove.models import ProcessorConfig


class BaseProcessor(abc.ABC):
    """Provides an abstract base processor which all processors must inherit from."""

    class Configuration(ProcessorConfig, extra=Extra.forbid):
        """Defines the required configuration and validators for the processor."""

        pass

    def __init__(self, config: Dict[str, Any]):
        """Sets up a Grove processor.

        :param config: The configuration document for this processor, as a dict.
        """
        self.logger = logging.getLogger(__name__)

        # Load and validate configuration. We perform a bit of a strange operation here
        # but our caller needs to have loaded the configuration into a ProcessorConfig
        # already, but we want to re-validate it here. As a result, we convert to a dict
        # and back again.
        try:
            self.configuration = self.Configuration(**config.dict())
        except ValidationError as err:
            raise ConfigurationException(
                f"Processor configuration is invalid. {parsing.validation_error(err)}"
            )

    def process(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Performs a set of processes against a log entry.

        :param entry: A collected log entry.

        :returns: The processed log entry in a list. If only a single entry is required
            the list should contain a single element. If the log entry is to be dropped,
            an empty list should be used.
        """
        return [entry]

    def finalize(self):
        """Performs a final set of operations after logs have been saved."""

        return

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides Grove configuration storage using supported backends."""

import abc
from typing import List

from grove.models import ConnectorConfig


class BaseConfig(abc.ABC):
    @abc.abstractmethod
    def get(self, id: str) -> List[ConnectorConfig]:
        """Gets and returns one or more connector configuration objects.

        Multiple connector configurations may be returned by backends which provide a
        recursive option where a less specific identifier is provided. This is used to
        enable lookup of all configuration documents under a "path" inside of the given
        backend - such as Consul, or AWS SSM.

        :param id: The identifier to use when querying for connector configuration.

        :return: A list of ConnectorConfig objects.
        """
        pass

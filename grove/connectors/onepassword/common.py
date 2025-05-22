# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""1Password connector for Grove."""

from typing import Optional

from cryptography.hazmat.primitives import serialization

from grove.connectors import BaseConnector
from grove.exceptions import ConfigurationException


API_DEFAULT_DOMAIN = "1password.com"  # Default to US endpoint

class OnePasswordConnector(BaseConnector):
    """Defines common fields used across all 1Password connector types.

    This has been done to reduce the amount of boilerplate and duplication across the
    different 1Password connectors in Grove.
    """

    @property
    def domain(self) -> str:
        """Fetches the domain from the configuration.

        :return: The "domain" portion of the connector's configuration.
        """
        try:
            return self.configuration.domain

        except AttributeError:
            return API_DEFAULT_DOMAIN

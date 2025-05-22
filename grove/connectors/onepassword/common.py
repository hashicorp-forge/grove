# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""1Password connector for Grove."""

from typing import Optional

from cryptography.hazmat.primitives import serialization

from grove.connectors import BaseConnector
from grove.exceptions import ConfigurationException


API_DEFAULT_DOMAIN = "1password.com"  # Default to US endpoint
VALID_DOMAINS = {
    "ent.1password.com",  # Enterprise US
    "1password.com",      # Business US
    "1password.eu",       # Business EU
    "1password.ca"        # Business Canada
}

class OnePasswordConnector(BaseConnector):
    """Defines common fields used across all 1Password connector types."""

    @property
    def domain(self) -> str:
        """Fetches and validates the domain from the configuration.
        
        :return: The validated domain from configuration or default
        :raises ConfigurationException: If configured domain is not valid
        """
        try:
            domain = self.configuration.domain
            if domain not in VALID_DOMAINS:
                raise ConfigurationException(
                    f"Invalid 1Password domain '{domain}'. "
                    f"Must be one of: {', '.join(VALID_DOMAINS)}"
                )
            return domain
            
        except AttributeError:
            return API_DEFAULT_DOMAIN

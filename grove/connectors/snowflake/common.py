# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Snowflake connector for Grove."""

from typing import Optional

from cryptography.hazmat.primitives import serialization

from grove.connectors import BaseConnector
from grove.exceptions import ConfigurationException


class SnowflakeConnector(BaseConnector):
    """Defines common fields used across all Snowflake connector types.

    This has been done to reduce the amount of boilerplate and duplication across the
    different Snowflake connectors in Grove.
    """

    def _load_private_key(self) -> bytes:
        """Loads and deserialises the configured PEM format PKCS#8 private key.

        :return: The private key in bytes (DER format).
        """
        passphrase = None
        if self.passphrase is not None:
            passphrase = bytes(self.passphrase, "utf-8")

        try:
            private_key = serialization.load_pem_private_key(
                bytes(self.key, "utf-8"),
                password=passphrase,
            )
            private_key_der = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        except (ValueError, TypeError) as err:
            raise ConfigurationException(
                f"Provided private key, or associated passphrase, is not valid. {err}"
            )

        return private_key_der

    @property
    def batch_size(self) -> int:
        """Fetches the batch size from the configuration.

        This is used to control the maximum number of records which will be retrieved
        before they are flushed to the output handler.

        The default is 500.

        :return: The "batch_size" portion of the connector's configuration.
        """
        try:
            candidate = self.configuration.batch_size
        except AttributeError:
            return 500

        try:
            candidate = int(candidate)
        except ValueError as err:
            raise ConfigurationException(
                f"Configured 'batch_size' is not valid. Value must be an integer. {err}"
            )

        return candidate

    @property
    def account(self) -> str:
        """Fetches the Snowflake account name from the configuration.

        :return: The "account" portion of the connector's configuration.
        """
        try:
            return self.configuration.account
        except AttributeError:
            raise ConfigurationException(
                "An account configuration file is required for the Snowflake connector"
            )

    @property
    def warehouse(self) -> Optional[str]:
        """Fetches the optional Snowflake warehouse name from the configuration.

        :return: The "warehouse" portion of the connector's configuration.
        """
        try:
            return self.configuration.warehouse
        except AttributeError:
            return None

    @property
    def role(self) -> Optional[str]:
        """Fetches the optional Snowflake role name from the configuration.

        :return: The "role" portion of the connector's configuration.
        """
        try:
            return self.configuration.role
        except AttributeError:
            return None

    @property
    def passphrase(self) -> Optional[str]:
        """Fetches the optional private key passphrase from the configuration.

        :return: The "passphrase" portion of the connector's configuration.
        """
        try:
            return self.configuration.passphrase
        except AttributeError:
            return None

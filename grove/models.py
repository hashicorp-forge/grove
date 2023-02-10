# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Data models used throughout Grove."""

import base64
import binascii
from typing import Dict

from pydantic import BaseModel, Extra, Field, root_validator, validator

from grove.constants import OPERATION_DEFAULT
from grove.exceptions import DataFormatException


def decode(value: str, encoding: str) -> str:
    """Decode a value using the specified encoding.

    :param value: The encoded value to decode.
    :param encoding: The encoding the value is encoded with.

    :raises DataFormatException: Decoding failed due to an issue with the data, or an
        due to an unsupported encoding method.

    :return: The decoded value.
    """
    # TODO: Decoded binary data would be cast to string here, this could present a
    # for a future unknown plugin or connector?
    try:
        if encoding == "base64" and value:
            return str(base64.b64decode(value), "utf-8")
    except binascii.Error as err:
        raise DataFormatException(f"Unable to base64 decode data, {err}")

    # More formats may be supported later.
    raise DataFormatException(f"Unknown encoding method '{encoding}'")


class ConnectorConfig(BaseModel, extra=Extra.allow):
    """A connector configuration object.

    A configuration object represents information which Grove uses to call a given
    connector. All connectors must have at least a name, key, identity, and connector
    defined.

    Connector configuration objects support the addition of arbitrary fields to enable
    service specific authentication and configuration concerns to be defined. This is
    useful for services which require more than one factor for authentication, or other
    tuneables such as self-hosted API instance FQDN.
    """

    name: str
    identity: str
    connector: str

    # Although key is required, it may be set via secret reference. This is checked
    # during validation.
    key: str = Field("")

    # Allow the connector to be disabled via configuration flag.
    disabled: bool = Field(False)

    # Secrets is used to mark which fields are considered to be secrets, and their
    # associated location in the configured secrets backend.
    secrets: Dict[str, str] = Field({})

    # Similar to secrets, Encoding is used to mark fields which are encoded due in some
    # form which must be decoded before use. This is often used for base64 encoding
    # binary data or nested JSON.
    encoding: Dict[str, str] = Field({})

    # Operations allow connectors and users to filter which 'type' of events to collect
    # from API endpoints which allow filtering records to return.
    operation: str = Field(OPERATION_DEFAULT)

    @validator("key")
    def _validate_key_or_secret(cls, value, values, field):  # noqa: B902
        """Ensures that 'key' is set directly or a reference is present in 'secrets'.

        This is used to ensure that a key is always set, whether directly, or will be
        fetched from the configured secrets backend at runtime (as indicated by an entry
        in the secrets field).
        """
        if value is None and field not in values["secrets"]:
            raise ValueError(f"Required field '{field}' is missing")

        return value

    @root_validator(pre=True)
    def _decode_fields(cls, values):  # noqa: B902
        """Automatically decode fields using the specified encoding during data loading.

        If a field is listed in both the 'secrets' field and this 'encoding' field,
        decoding will be deferred to after secrets have been retrieved. This allows
        encoding of externally stored secrets to be specified using this field.

        This is intended to allow fields which contain data that is not easily expressed
        in JSON to be encoded with an appropriate scheme. As an example, this is useful
        to allow binary or multi-line data by first encoding the value with base64.

        Other encoding schemes may be supported in future, but for now only base64 is
        supported.
        """
        for field, encoding in values.get("encoding", {}).items():
            # If the secret is externally stored decoding will be performed after the
            # secret has been retrieved. Right now, this field should not exist as it
            # won't have been fetched yet.
            if field in values.get("secrets", {}):
                continue

            values[field] = decode(values.get(field), encoding)

        return values

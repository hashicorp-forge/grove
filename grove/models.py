# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Data models used throughout Grove."""

import base64
import binascii
import datetime
import hashlib
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Extra, Field, root_validator, validator

from grove.constants import (
    DEFAULT_CONFIG_FREQUENCY,
    DEFAULT_OPERATION,
)
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


class ProcessorConfig(BaseModel, extra=Extra.allow):
    """A processor configuration object.

    A processor configuration object represents information used by processors to
    perform some set of operations on log entries. This base configuration object
    is bare-bones as processors may define their own required configuration fields.
    """

    # Name is an arbitrary name administrators can provide to processors to enable
    # better tracking and identification of processors.
    name: str

    # Processor defines the processor which should be run. This must match the plugin
    # entrypoint name.
    processor: str


class OutputStream(str, Enum):
    """Defines supported output 'streams'.

    This is used to allow routing of original / raw collected data differently to
    post processed data.
    """

    raw = "raw"
    processed = "processed"


class ConnectorConfig(BaseModel, extra=Extra.allow):
    """Defines the connector configuration structure.

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
    operation: str = Field(DEFAULT_OPERATION)

    # Frequency to execute connector if not explicitly configured.
    frequency: int = Field(DEFAULT_CONFIG_FREQUENCY)

    # Processors allow processing of data during collection.
    processors: List[ProcessorConfig] = Field([])

    # Outputs allows specification of what type of data to output, and with what
    # descriptor. By default, any processed logs will be output with a descriptor of
    # 'processed', and raw logs with a descriptor of 'logs'.
    outputs: Dict[str, OutputStream] = Field(
        {
            "logs": OutputStream.raw,
            "processed": OutputStream.processed,
        }
    )

    def reference(self, suffix: Optional[str] = None) -> str:
        """Attempt to generate a unique reference for this connector instance.

        This is used during creation of cache keys, and other values which should be
        unique per connector instance.

        :param suffix: An optional suffix to append to the end of the reference. This is
            is useful for handling other configuration data to the reference, such as
            the operation.
        """
        # MD5 may not be cryptographically secure, but it works for our purposes. It's:
        #
        #   1) Short.
        #   2) Has a low chance of (non-deliberate) collisions.
        #   3) Able to be 'stringified' as hex, the character set of which is compatible
        #      with backends like DynamoDB.
        #
        parts = [
            self.connector,
            hashlib.md5(bytes(self.identity, "utf-8")).hexdigest(),
        ]
        if suffix is not None:
            parts.append(suffix)

        return ".".join(parts)

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
        # This is a horrible hack to allow fields with names that mask Pydantic
        # internals. This can be removed once Grove is updated to use Pydantic >= 2.
        INTERNAL_FIELDS = ["schema"]

        for field in INTERNAL_FIELDS:
            value = values.get(field, None)
            if value is None:
                continue

            # Remap the field name to contain a trailing underscore.
            values[f"{field}_"] = value
            del values[field]

        for field, encoding in values.get("encoding", {}).items():
            # If the secret is externally stored decoding will be performed after the
            # secret has been retrieved. Right now, this field should not exist as it
            # won't have been fetched yet.
            if field in values.get("secrets", {}):
                continue

            values[field] = decode(values.get(field), encoding)

        return values


class Run(BaseModel, extra=Extra.forbid):
    """Defines a model for tracking dispatched / running connectors.

    This is used when running as a daemon, in order to allow local tracking of state
    and to prevent the need to constantly hit the cache backend during the main
    event loop.
    """

    # The future associated with the dispatched thread, or runtime element.
    future: Optional[Any] = None

    # The connector configuration for this run.
    configuration: ConnectorConfig

    # A date-time object representing the last time this was dispatched.
    last: Optional[datetime.datetime] = None

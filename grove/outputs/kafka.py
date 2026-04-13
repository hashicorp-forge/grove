# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove Kafka output handler."""

import json
from typing import Any, Dict, List, Optional

from pydantic import Field

from grove.constants import GROVE_METADATA_KEY
from grove.exceptions import AccessException, ConfigurationException, DataFormatException
from grove.outputs import BaseOutput


class Handler(BaseOutput):
    """Output handler to publish Grove logs to Apache Kafka or Confluent Platform."""

    class Configuration(BaseOutput.Configuration):
        """Defines environment variables used to configure the Kafka handler.

        This should also include any appropriate default values for fields which are not
        required.
        """

        bootstrap_servers: str = Field(
            description="Comma-separated list of Kafka broker addresses (host:port).",
        )
        topic: str = Field(
            description="Default Kafka topic to publish log data to.",
        )
        security_protocol: str = Field(
            description="Protocol used to communicate with Kafka brokers.",
            default="PLAINTEXT",
        )
        sasl_mechanism: Optional[str] = Field(
            description=(
                "SASL mechanism for authentication (e.g. PLAIN, SCRAM-SHA-256, "
                "OAUTHBEARER)."
            ),
            default=None,
        )
        sasl_username: Optional[str] = Field(
            description="SASL username for broker authentication.",
            default=None,
        )
        sasl_password: Optional[str] = Field(
            description="SASL password for broker authentication.",
            default=None,
        )
        ssl_ca_location: Optional[str] = Field(
            description="Path to a CA certificate file for TLS verification.",
            default=None,
        )
        ssl_certificate_location: Optional[str] = Field(
            description="Path to a client certificate file for mutual TLS.",
            default=None,
        )
        ssl_key_location: Optional[str] = Field(
            description="Path to a client private key file for mutual TLS.",
            default=None,
        )
        max_records_per_message: int = Field(
            description="Maximum number of log records per Kafka message.",
            default=500,
        )
        max_bytes_per_message: int = Field(
            description="Maximum serialized size in bytes per Kafka message.",
            default=750000,
        )
        compression_type: str = Field(
            description=(
                "Compression codec applied by the Kafka producer "
                "(none, gzip, snappy, lz4, zstd)."
            ),
            default="lz4",
        )
        flush_timeout: int = Field(
            description=(
                "Seconds to wait for delivery confirmation before raising an error."
            ),
            default=30,
        )
        use_identity_as_key: bool = Field(
            description="Whether to use the connector identity as the Kafka message key.",
            default=False,
        )

        class Config:
            """Allow environment variable override of configuration fields.

            This also enforces a prefix for all environment variables for this handler.
            As an example the field `bootstrap_servers` would be set using the
            environment variable `GROVE_OUTPUT_KAFKA_BOOTSTRAP_SERVERS`.
            """

            env_prefix = "GROVE_OUTPUT_KAFKA_"
            case_insensitive = True

    def setup(self):
        """Instantiates the Kafka producer using the resolved configuration.

        :raises ConfigurationException: The confluent-kafka package is not installed,
            or the producer could not be created with the supplied configuration.
        """
        try:
            from confluent_kafka import BufferError as KafkaBufferError  # type: ignore
            from confluent_kafka import Producer  # type: ignore
        except ImportError:
            raise ConfigurationException(
                "The confluent-kafka package is required for the Kafka output handler. "
                "Install it with: pip install grove[kafka]"
            )

        # Store for use in _produce_chunk without re-importing.
        self._KafkaBufferError = KafkaBufferError

        conf: Dict[str, Any] = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "security.protocol": self.config.security_protocol,
            "compression.type": self.config.compression_type,
        }

        if self.config.sasl_mechanism:
            conf["sasl.mechanism"] = self.config.sasl_mechanism
        if self.config.sasl_username:
            conf["sasl.username"] = self.config.sasl_username
        if self.config.sasl_password:
            conf["sasl.password"] = self.config.sasl_password
        if self.config.ssl_ca_location:
            conf["ssl.ca.location"] = self.config.ssl_ca_location
        if self.config.ssl_certificate_location:
            conf["ssl.certificate.location"] = self.config.ssl_certificate_location
        if self.config.ssl_key_location:
            conf["ssl.key.location"] = self.config.ssl_key_location

        try:
            self._producer = Producer(conf)
        except Exception as err:
            raise ConfigurationException(f"Failed to create Kafka producer: {err}")

    def _on_delivery(self, err: Any, msg: Any):
        """Records delivery errors reported by the Kafka producer.

        :param err: A KafkaError instance if delivery failed, or None on success.
        :param msg: The delivered Message object.
        """
        if err is not None:
            self._delivery_errors.append(str(err))

    def _produce_chunk(self, topic: str, value: bytes, key: Optional[bytes]):
        """Produces a single message chunk to Kafka, flushing on buffer overflow.

        :param topic: Kafka topic to produce to.
        :param value: Serialized message payload.
        :param key: Optional message key bytes.

        :raises AccessException: A non-recoverable producer error occurred.
        """
        try:
            self._producer.produce(
                topic,
                value=value,
                key=key,
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)
        except self._KafkaBufferError:
            # Internal queue is full; flush outstanding messages then retry once.
            self._producer.flush(self.config.flush_timeout)
            self._producer.produce(
                topic,
                value=value,
                key=key,
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)
        except Exception as err:
            raise AccessException(f"Failed to produce Kafka message: {err}")

    def submit(
        self,
        data: bytes,
        connector: str,
        identity: str,
        operation: str,
        part: int = 0,
        suffix: Optional[str] = None,
        descriptor: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """Publishes collected log data to a Kafka topic as NDJSON messages.

        Each call to submit may produce one or more Kafka messages depending on the
        number of log records and the configured byte and record thresholds.

        :param data: Log data to publish (plain NDJSON bytes as returned by serialize).
        :param connector: Name of the connector which retrieved the data.
        :param identity: Identity the collected data was collected for.
        :param operation: Operation the collected logs are associated with.
        :param part: Number indicating which part of the same log stream this data
            contains logs for.
        :param suffix: Currently not used by this output handler.
        :param descriptor: Optional topic name override. When provided, log data is
            published to this topic instead of the configured default topic.
        :param name: Given name of the connector as specified in the user's configuration.

        :raises AccessException: A message could not be delivered to the broker.
        """
        topic = descriptor if descriptor else self.config.topic
        key: Optional[bytes] = (
            bytes(identity, "utf-8") if self.config.use_identity_as_key else None
        )

        # Reset delivery errors for this submit call.
        self._delivery_errors: List[str] = []

        # Split NDJSON payload into individual record lines.
        lines = [line for line in data.split(b"\r\n") if line]

        # Chunk lines by max_records_per_message and max_bytes_per_message.
        chunk: List[bytes] = []
        chunk_bytes = 0

        for line in lines:
            line_size = len(line) + 2  # +2 accounts for the \r\n separator

            # Flush current chunk if adding this line would exceed either threshold.
            if chunk and (
                len(chunk) >= self.config.max_records_per_message
                or chunk_bytes + line_size > self.config.max_bytes_per_message
            ):
                self._produce_chunk(topic, b"\r\n".join(chunk), key)
                chunk = []
                chunk_bytes = 0

            chunk.append(line)
            chunk_bytes += line_size

        # Produce the final (possibly partial) chunk.
        if chunk:
            self._produce_chunk(topic, b"\r\n".join(chunk), key)

        # Wait for all in-flight messages to be delivered.
        remaining = self._producer.flush(self.config.flush_timeout)
        if remaining > 0:
            raise AccessException(
                f"Kafka flush timed out with {remaining} message(s) still undelivered."
            )

        if self._delivery_errors:
            errors = "; ".join(self._delivery_errors)
            raise AccessException(
                f"One or more Kafka messages failed delivery: {errors}"
            )

    def serialize(self, data: List[Any], metadata: Dict[str, Any] = {}) -> bytes:
        """Implements serialization of log entries to plain NDJSON.

        Compression is delegated to the Kafka producer via the configured
        compression_type, so the payload returned here is not compressed.

        :param data: A list of log entries to serialize to JSON.
        :param metadata: Metadata to append to each log entry before serialization. If
            not specified no metadata will be added.

        :return: Log data serialized as NDJSON (as bytes).

        :raises DataFormatException: Cannot serialize the input to JSON.
        """
        candidate = []

        for entry in data:
            # Skip empty log entries.
            if entry is None:
                continue

            if metadata:
                entry[GROVE_METADATA_KEY] = {
                    **metadata,
                    **entry.get(GROVE_METADATA_KEY, {}),
                }

            # We don't want to silently drop and lose single records, so drop the entire
            # batch if there is bad data (which will trigger a retry next run).
            try:
                candidate.append(json.dumps(entry, separators=(",", ":"), default=str))
            except TypeError as err:
                raise DataFormatException(f"Unable to serialize to JSON: {err}")

        return bytes("\r\n".join(candidate), "utf-8")

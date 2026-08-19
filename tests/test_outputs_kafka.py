# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Implements tests for the Kafka output handler."""

import os
import sys
import unittest
from unittest.mock import MagicMock

# Provide a mock confluent_kafka module before the handler is imported, since it is an
# optional dependency and may not be installed in the test environment.
_mock_confluent_kafka = MagicMock()
_mock_confluent_kafka.BufferError = type("BufferError", (Exception,), {})
sys.modules["confluent_kafka"] = _mock_confluent_kafka

from grove.exceptions import AccessException, ConfigurationException  # noqa: E402
from grove.outputs.kafka import Handler  # noqa: E402


class KafkaOutputTestCase(unittest.TestCase):
    """Implements tests for the Kafka output handler."""

    def setUp(self):
        os.environ["GROVE_OUTPUT_KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
        os.environ["GROVE_OUTPUT_KAFKA_TOPIC"] = "grove-logs"

        self.mock_producer = MagicMock()
        self.mock_producer.flush.return_value = 0
        _mock_confluent_kafka.Producer.return_value = self.mock_producer

        self.handler = Handler()
        self.handler.setup()

    def tearDown(self):
        for key in [
            "GROVE_OUTPUT_KAFKA_BOOTSTRAP_SERVERS",
            "GROVE_OUTPUT_KAFKA_TOPIC",
            "GROVE_OUTPUT_KAFKA_SECURITY_PROTOCOL",
            "GROVE_OUTPUT_KAFKA_SASL_MECHANISM",
            "GROVE_OUTPUT_KAFKA_SASL_USERNAME",
            "GROVE_OUTPUT_KAFKA_SASL_PASSWORD",
            "GROVE_OUTPUT_KAFKA_MAX_RECORDS_PER_MESSAGE",
            "GROVE_OUTPUT_KAFKA_MAX_BYTES_PER_MESSAGE",
            "GROVE_OUTPUT_KAFKA_USE_IDENTITY_AS_KEY",
        ]:
            os.environ.pop(key, None)

    def test_setup_success(self):
        """Ensures the Kafka producer is created with correct configuration."""
        call_kwargs = _mock_confluent_kafka.Producer.call_args[0][0]

        self.assertEqual(call_kwargs["bootstrap.servers"], "localhost:9092")
        self.assertEqual(call_kwargs["security.protocol"], "PLAINTEXT")
        self.assertEqual(call_kwargs["compression.type"], "lz4")

    def test_setup_missing_bootstrap_servers(self):
        """Ensures ConfigurationException is raised when bootstrap_servers is absent."""
        os.environ.pop("GROVE_OUTPUT_KAFKA_BOOTSTRAP_SERVERS")

        with self.assertRaises(ConfigurationException):
            Handler()

    def test_setup_missing_topic(self):
        """Ensures ConfigurationException is raised when topic is absent."""
        os.environ.pop("GROVE_OUTPUT_KAFKA_TOPIC")

        with self.assertRaises(ConfigurationException):
            Handler()

    def test_setup_includes_sasl_when_configured(self):
        """Ensures SASL fields are included in the producer config when set."""
        os.environ["GROVE_OUTPUT_KAFKA_SASL_MECHANISM"] = "PLAIN"
        os.environ["GROVE_OUTPUT_KAFKA_SASL_USERNAME"] = "user"
        os.environ["GROVE_OUTPUT_KAFKA_SASL_PASSWORD"] = "pass"

        handler = Handler()
        handler.setup()

        call_kwargs = _mock_confluent_kafka.Producer.call_args[0][0]
        self.assertEqual(call_kwargs["sasl.mechanism"], "PLAIN")
        self.assertEqual(call_kwargs["sasl.username"], "user")
        self.assertEqual(call_kwargs["sasl.password"], "pass")

    def test_submit_success(self):
        """Ensures produce and flush are called on a successful submit."""
        data = bytes('{"id":"001","name":"One"}\r\n{"id":"002","name":"Two"}', "utf-8")

        self.handler.submit(
            data=data,
            connector="test_connector",
            identity="test_identity",
            operation="test_operation",
        )

        self.mock_producer.produce.assert_called_once()
        self.mock_producer.flush.assert_called_once_with(30)

    def test_submit_uses_descriptor_as_topic(self):
        """Ensures descriptor overrides the default topic when producing messages."""
        data = bytes('{"id":"001"}', "utf-8")

        self.handler.submit(
            data=data,
            connector="X",
            identity="Y",
            operation="Z",
            descriptor="custom-topic",
        )

        topic_arg = self.mock_producer.produce.call_args[0][0]
        self.assertEqual(topic_arg, "custom-topic")

    def test_submit_uses_default_topic_when_no_descriptor(self):
        """Ensures the configured topic is used when no descriptor is supplied."""
        data = bytes('{"id":"001"}', "utf-8")

        self.handler.submit(
            data=data,
            connector="X",
            identity="Y",
            operation="Z",
        )

        topic_arg = self.mock_producer.produce.call_args[0][0]
        self.assertEqual(topic_arg, "grove-logs")

    def test_submit_chunks_by_record_count(self):
        """Ensures submit splits data when max_records_per_message is exceeded."""
        os.environ["GROVE_OUTPUT_KAFKA_MAX_RECORDS_PER_MESSAGE"] = "2"
        handler = Handler()
        handler.setup()

        lines = [f'{{"id":"{i}"}}' for i in range(5)]
        data = bytes("\r\n".join(lines), "utf-8")

        handler.submit(
            data=data,
            connector="X",
            identity="Y",
            operation="Z",
        )

        # 5 records with max 2 per message → 3 produce calls (2 + 2 + 1).
        self.assertEqual(self.mock_producer.produce.call_count, 3)

    def test_submit_chunks_by_byte_size(self):
        """Ensures submit splits data when max_bytes_per_message is exceeded."""
        os.environ["GROVE_OUTPUT_KAFKA_MAX_BYTES_PER_MESSAGE"] = "20"
        handler = Handler()
        handler.setup()

        # Each line is {"id":"0000"} = 13 bytes; line_size = 15 with separator.
        # max_bytes=20: each chunk holds exactly one line (15 < 20; 15+15=30 > 20).
        lines = [f'{{"id":"{i:04d}"}}' for i in range(4)]
        data = bytes("\r\n".join(lines), "utf-8")

        handler.submit(
            data=data,
            connector="X",
            identity="Y",
            operation="Z",
        )

        # 4 records, one per chunk → 4 produce calls.
        self.assertEqual(self.mock_producer.produce.call_count, 4)

    def test_submit_raises_on_delivery_error(self):
        """Ensures AccessException is raised when a delivery callback reports failure."""

        def side_effect(topic, value=None, key=None, on_delivery=None):
            if on_delivery is not None:
                on_delivery("simulated delivery error", MagicMock())

        self.mock_producer.produce.side_effect = side_effect

        data = bytes('{"id":"001"}', "utf-8")

        with self.assertRaises(AccessException):
            self.handler.submit(
                data=data,
                connector="X",
                identity="Y",
                operation="Z",
            )

    def test_submit_raises_on_flush_timeout(self):
        """Ensures AccessException is raised when flush returns undelivered messages."""
        self.mock_producer.flush.return_value = 1

        data = bytes('{"id":"001"}', "utf-8")

        with self.assertRaises(AccessException):
            self.handler.submit(
                data=data,
                connector="X",
                identity="Y",
                operation="Z",
            )

    def test_submit_uses_identity_as_key(self):
        """Ensures the connector identity is used as the Kafka message key."""
        os.environ["GROVE_OUTPUT_KAFKA_USE_IDENTITY_AS_KEY"] = "true"
        handler = Handler()
        handler.setup()

        data = bytes('{"id":"001"}', "utf-8")

        handler.submit(
            data=data,
            connector="X",
            identity="my-identity",
            operation="Z",
        )

        produce_kwargs = self.mock_producer.produce.call_args[1]
        self.assertEqual(produce_kwargs["key"], b"my-identity")

    def test_submit_no_key_by_default(self):
        """Ensures no message key is set when use_identity_as_key is False."""
        data = bytes('{"id":"001"}', "utf-8")

        self.handler.submit(
            data=data,
            connector="X",
            identity="Y",
            operation="Z",
        )

        produce_kwargs = self.mock_producer.produce.call_args[1]
        self.assertIsNone(produce_kwargs["key"])

    def test_serialize_returns_plain_ndjson(self):
        """Ensures serialization produces plain NDJSON bytes, not gzipped."""
        candidate = [
            {"id": "0001", "name": "One"},
            {"id": "0002", "name": "Two"},
            {"id": "0003", "name": "Three"},
        ]

        expected_raw = "\r\n".join(
            [
                '{"id":"0001","name":"One","_grove":{"field":"value"}}',
                '{"id":"0002","name":"Two","_grove":{"field":"value"}}',
                '{"id":"0003","name":"Three","_grove":{"field":"value"}}',
            ]
        )
        expected_raw = bytes(expected_raw, "utf-8")

        result = self.handler.serialize(data=candidate, metadata={"field": "value"})

        self.assertEqual(expected_raw, result)
        # Verify it is not gzip-compressed (gzip magic bytes are 0x1f 0x8b).
        self.assertFalse(result[:2] == b"\x1f\x8b")

    def test_serialize_skips_none_entries(self):
        """Ensures None entries are omitted from the serialized output."""
        candidate = [{"id": "001"}, None, {"id": "002"}]
        result = self.handler.serialize(data=candidate)
        lines = result.split(b"\r\n")
        self.assertEqual(len(lines), 2)

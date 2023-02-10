# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides a consistent logger in Grove."""


import json
import logging
from typing import Any, Dict

from aws_lambda_powertools.logging.formatter import RESERVED_LOG_ATTRS, JsonFormatter


class GroveFormatter(JsonFormatter):
    """A logging formatter which emits logs in a consistent and structured way.

    This formatter emits logs in JSON, with all "context" provided at creation added to
    each emitted message, alongside any "extra" data provided during logging calls. This
    formatter also adds the function name, file name, and line number of the log call to
    each message.
    """

    def __init__(self, context: Dict[str, str], *args, **kwargs):
        self.utc = True
        self.context = context

        super().__init__(*args, **kwargs)

        # Add the function name the code to the code, including line-number, to all
        # messages to enable easier location of the source of any issues.
        self.reserved_attrs = (*RESERVED_LOG_ATTRS, "function")

        self.log_format["location"] = "%(pathname)s:%(lineno)d"
        self.log_format["function"] = "%(funcName)s"

    def extract_keys(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Extracts and formats log records into dictionaries ready for serialisation.

        This is heavily based on the code from the AWS Lambda PowerTools formatter that
        this formatter inherits from.

        :param record: A log record to process.

        :return: A dictionary of log data to be serialized and output.
        """
        extras = {}
        structured = record.__dict__.copy()
        structured["asctime"] = self.formatTime(record=record)

        # Determine which arguments are "extras" provided by the caller.
        for key, value in structured.items():
            if key not in self.reserved_attrs:
                extras[key] = value

        # Format all fields according to their specified format strings.
        formatted = {}

        for key, value in self.log_format.items():
            if value and key in self.reserved_attrs:
                formatted[key] = value % structured
            else:
                formatted[key] = value

        # Stuff all 'extras' provided to a logging call into a field called "details".
        formatted.update({"detail": extras})

        return formatted

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        """Return the log message including any context provided by the entrypoint.

        :param record: A log record to process.

        :return: A stringified JSON document rendered from the log record.
        """
        structured = {}

        # Remove any records which have a value of None.
        candidate = self.extract_keys(record=record)
        candidate["message"] = str(record.msg)
        candidate["context"] = self.context

        for key, value in candidate.items():
            if value is not None:
                structured[key] = value

        return json.dumps(structured, default=str)

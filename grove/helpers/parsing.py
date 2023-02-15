# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides helpers for parsing."""

from pydantic import ValidationError


def validation_error(exc: ValidationError):
    """Parse Pydantic validation exceptions into a user readable string.

    :param exc: The Pydantic ValidationError to parse.

    :return: The exception as a string, including fields with validation errors.
    """
    prefix = exc.model.Config.env_prefix  # type: ignore
    message = "Handler configuration is not valid"

    # Ensure the validation errors are included in the logged error message.
    for error in exc.errors():
        field = error["loc"][0].upper()  # type: ignore
        problem = error["msg"]

        # Add the environment variable prefix onto the field name.
        message = f"{message}, {prefix}{field} {problem}"

    return message

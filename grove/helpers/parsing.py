# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides helpers for parsing."""

import json
import re
from typing import Any, Dict, List

from pydantic import ValidationError


def validation_error(exc: ValidationError):
    """Parse Pydantic validation exceptions into a user readable string.

    :param exc: The Pydantic ValidationError to parse.

    :return: The exception as a string, including fields with validation errors.
    """
    message = "Handler configuration is not valid"
    try:
        prefix = exc.model.Config.env_prefix  # type: ignore
    except AttributeError:
        prefix = ""

    # Ensure the validation errors are included in the logged error message.
    for error in exc.errors():
        field = str(error["loc"][0]).upper()
        problem = error["msg"]

        # Add the environment variable prefix onto the field name.
        message = f"{message}, {prefix}{field} {problem}"

    return message


def quick_copy(value: Any):
    """Performs a quick deep copy by marshalling and unmarshalling to JSON.

    This operation, although strange, is significantly quicker than copy.deepcopy().
    This has been moved into a helper to enable potential performance improvements in
    future without code changes in processors being required.

    :param value: The value to perform a deep copy of.

    :return: The deep copy of the input value.
    """
    return json.loads(json.dumps(value))


def quote_aware_split(value: str, delimiter=".") -> List[str]:
    """Splits a value by delimiter, returning a list.

    This function is quote aware, ensuring that splitting will not occur inside of a
    value quoted with single-quotes.

    :param value: The value to split.
    :param delimiter: The delimiter to split using.

    :return: A list of elements split from the input value.
    """
    fields = []

    for field in re.split(rf"({re.escape(delimiter)}|'.*?')", value):
        # Drop empty and delimiter only fields.
        field = field.strip(delimiter)
        field = field.strip()
        field = re.sub(r"^'(.*)'$", r"\1", field)

        if field:
            fields.append(field)

    return fields


def update_path(
    candidate: Dict[str, Any],
    path: List[str],
    value: Any,
    replace: bool = False,
) -> Dict[str, Any]:
    """Updates or deletes values under the specified path for the provided candidate.

    A path is a list of strings delimited string which express a location within the
    candidate data. If the location is not nested, a single element list should be
    provided.

    As an example of this, a path of `["A", "B", "C"]` expresses that the specified
    value should be set under `{"A": {"B": {"C": value } } }` within the candidate
    dictionary.

    This function recursively walks the provided candidate dictionary until the location
    specified by the path has been located. Once found, the provided value will perform
    on of the following operations:

        1. By default, the provided value will be combined with the existing value.
        2. If `replace` is `True`, the existing value will be replaced with the new.
        3. If `None` is provided as the new value, the specified path will be deleted.

    :param candidate: The dictionary to update.
    :param path: The path to the key to update, as a list of strings.
    :param value: The value to assign to the destination path, or None to delete.
    :param replace: Whether to replace the current value with the new value, or combine.

    :return: The updated dictionary.
    """
    key = path.pop(0)

    # Set the value on the last recursion.
    if len(path) < 1:
        if value is None:
            del candidate[key]
            return candidate

        # If replace is set, don't combine the new value with the existing.
        if replace:
            candidate[key] = value
            return candidate

        # By default, combine the new value with the existing value(s) - making sure to
        # handle dictionaries as well as lists.
        if key in candidate and isinstance(candidate[key], list):
            candidate[key].append(value)
        else:
            candidate = {**candidate, key: value}

        return candidate

    # If recursing, ensure the child we're trying to recurse into exists.
    if key not in candidate:
        candidate[key] = {}

    candidate[key] = update_path(candidate[key], path, value, replace=replace)

    return candidate

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Custom types used throughout Grove."""

from collections.abc import MutableMapping
from typing import Any, Dict, List, NamedTuple, Optional, Union


class HTTPResponse(NamedTuple):
    """Provides both the headers and the body of an HTTP response."""

    headers: MutableMapping  # type: ignore
    body: Dict[str, Any]


class AuditLogEntries(NamedTuple):
    """Provides both a pagination cursor and entries from an API response."""

    cursor: Union[Optional[str], Optional[int]]
    entries: List[Any]

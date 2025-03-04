# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

from datetime import datetime, timedelta
from grove.exceptions import NotFoundException


def get_pointer_values(connector):
    """Gets the start_time and cursor values to use for queries.

    Checks to see if a previous pointer has been set. If so, it checks to see if it's
    a date (pointer was previously set in cache and not upgraded) and if not assumes
    it's a cursor. If the pointer is not found, it sets a date of 7 days ago as a starting
    point

    :param connector: Instance of the 1Password connector (we use logger and pointer).
    :return: A tuple containing (cursor, start_time).
        - cursor - opaque string representing an index into 1Password's event stream. If
        we can't parse the cached pointer as a time, we assume it's a cursor. Returns None
        if start_time is present.
        - start_time - Time parsed from cached pointer. Returns None if
        cursor is present. If there's no cached pointer, default to 7 days ago.
    """

    cursor = None
    start_time = None
    try:
        _ = connector.pointer
    except NotFoundException:
        week_ago = datetime.now() - timedelta(days=7)
        start_time = (week_ago).astimezone().replace(microsecond=0).isoformat()
        return cursor, start_time

    # originally, we used the timestamp for the pointer. Check to see we have a timestamp
    # for backwards compatibility. Otherwise interpet it as a cursor.
    try:
        datetime.fromisoformat(connector.pointer)
    except ValueError:
        connector.logger.debug(
            "Pointer appears to already be a cursor.", extra={"cursor": cursor}
        )
        cursor = connector.pointer
    else:
        connector.logger.debug(
            "Pointer appears to be a timestamp.",
            extra={"cursor": cursor},
        )
        start_time = connector.pointer
    return cursor, start_time

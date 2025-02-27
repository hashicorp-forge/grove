# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

from datetime import datetime, timedelta
from grove.exceptions import NotFoundException


def get_pointer_values(connector):
    """Gets the start_time and cursor values to use for queries.

    Checks to see if a previous pointer has been set. If so, it checks to see if it's
    a date (pointer was previously set in cache and not upgraded) and if not assumes
    it's a cursor. If the pointer is not found, it sets a date of 7 days ago as a starting
    point"""

    try:
        _ = connector.pointer
        if not connector.pointer:
            raise NotFoundException
    except NotFoundException:
        week_ago = datetime.now() - timedelta(days=7)
        start_time = (week_ago).astimezone().replace(microsecond=0).isoformat()
        return None, start_time

    cursor = ""
    start_time = ""

    # originally, we used the timestamp for the pointer. Check to see we have a timestamp
    # for backwards compatibility. Otherwise interpet it as a cursor.
    try:
        datetime.fromisoformat(connector.pointer)
    except ValueError:
        connector.logger.info("Pointer is already a cursor")
        cursor = connector.pointer
    else:
        connector.logger.info("Pointer is a timestamp.")
        start_time = connector.pointer
    return cursor, start_time

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Google BigQuery utility functions for Grove."""

from datetime import datetime, timezone


def as_bigquery_timestamp_microseconds(epoch_usec) -> str:
    """
    Converts epoch time in microseconds to a BigQuery-compatible timestamp string.

    :param epoch_usec: The epoch time in microseconds (int, str, or float).
    :return: A BigQuery TIMESTAMP formatted date string (YYYY-MM-DD HH:MM:SS+00).
    """
    # Convert to int if it's a string
    if isinstance(epoch_usec, str):
        epoch_usec = int(epoch_usec)
    
    # Convert microseconds to seconds
    timestamp_seconds = epoch_usec / 1_000_000.0
    
    dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
    # BigQuery expects "+00" not "+0000" or "+00:00"
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


def as_bigquery_timestamp_seconds(epoch_sec) -> str:
    """
    Converts epoch time in seconds to a BigQuery-compatible timestamp string.

    :param epoch_sec: The epoch time in seconds (int, str, or float).
    :return: A BigQuery TIMESTAMP formatted date string (YYYY-MM-DD HH:MM:SS+00).
    """
    # Convert to float if it's a string
    if isinstance(epoch_sec, str):
        epoch_sec = float(epoch_sec)
    
    dt = datetime.fromtimestamp(epoch_sec, tz=timezone.utc)
    # BigQuery expects "+00" not "+0000" or "+00:00"
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


def as_bigquery_timestamp_milliseconds(epoch_msec) -> str:
    """
    Converts epoch time in milliseconds to a BigQuery-compatible timestamp string.

    :param epoch_msec: The epoch time in milliseconds (int, str, or float).
    :return: A BigQuery TIMESTAMP formatted date string (YYYY-MM-DD HH:MM:SS+00).
    """
    # Convert to int if it's a string
    if isinstance(epoch_msec, str):
        epoch_msec = int(epoch_msec)
    
    # Convert milliseconds to seconds
    timestamp_seconds = epoch_msec / 1_000.0
    
    dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
    # BigQuery expects "+00" not "+0000" or "+00:00"
    return dt.strftime("%Y-%m-%d %H:%M:%S+00") 
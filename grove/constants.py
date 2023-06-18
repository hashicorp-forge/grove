# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Constants used throughout Grove."""

# The prefix for all pointers in the cache.
CACHE_KEY_LOCK = "execution_lock"
CACHE_KEY_SEEN = "deduplication"
CACHE_KEY_POINTER = "pointer"
CACHE_KEY_POINTER_NEXT = "pointer_next"
CACHE_KEY_POINTER_PREV = "pointer_previous"

# The prefix for window start pointers.
CACHE_KEY_WINDOW_START = "window_start"
CACHE_KEY_WINDOW_END = "window_end"

# The default operation name to use where none is specified.
OPERATION_DEFAULT = "all"

# The common datestamp format to use for all date operations.
DATESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
LOCK_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# Constants for tracking ordering of source log data.
CHRONOLOGICAL = "CHRONOLOGICAL"
REVERSE_CHRONOLOGICAL = "REVERSE_CHRONOLOGICAL"

# Specify the key under which grove metadata will be appended to a log entry.
GROVE_METADATA_KEY = "_grove"

# Environment variable names, used to override runtime settings.
ENV_GROVE_CACHE_HANDLER = "GROVE_CACHE_HANDLER"
ENV_GROVE_OUTPUT_HANDLER = "GROVE_OUTPUT_HANDLER"
ENV_GROVE_CONFIG_HANDLER = "GROVE_CONFIG_HANDLER"
ENV_GROVE_SECRET_HANDLER = "GROVE_SECRET_HANDLER"  # noqa: S105
ENV_GROVE_TELEMETRY_URI = "GROVE_TELEMETRY_URI"
ENV_GROVE_WORKER_COUNT = "GROVE_WORKER_COUNT"
ENV_GROVE_LOCK_DURATION = "GROVE_LOCK_DURATION"

# Plugin groups (setuptools entrypoints).
PLUGIN_GROUP_CACHE = "grove.caches"
PLUGIN_GROUP_OUTPUT = "grove.outputs"
PLUGIN_GROUP_CONFIG = "grove.configs"
PLUGIN_GROUP_PROCESSOR = "grove.processors"
PLUGIN_GROUP_SECRET = "grove.secrets"  # noqa: S105
PLUGIN_GROUP_CONNECTOR = "grove.connectors"

# Define defines for unset environment variables.
DEFAULT_CACHE_HANDLER = "local_memory"
DEFAULT_OUTPUT_HANDLER = "local_stdout"
DEFAULT_CONFIG_HANDLER = "local_file"

# Maximum number of connectors to execute concurrently.
DEFAULT_WORKER_COUNT = 50
DEFAULT_LOCK_DURATION = 300  # seconds.

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Constants used throughout Grove."""

# The prefix for all pointers in the cache.
CACHE_KEY_LAST = "last_run"
CACHE_KEY_LOCK = "execution_lock"
CACHE_KEY_SEEN = "deduplication"
CACHE_KEY_POINTER = "pointer"
CACHE_KEY_POINTER_NEXT = "pointer_next"
CACHE_KEY_POINTER_PREV = "pointer_previous"
CACHE_KEY_ZERO_VOLUME = "zero_volume_runs"

# The prefix for window start pointers.
CACHE_KEY_WINDOW_START = "window_start"
CACHE_KEY_WINDOW_END = "window_end"

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
ENV_GROVE_OBSERVER_HANDLER = "GROVE_OBSERVER_HANDLER"
ENV_GROVE_CONFIG_HANDLER = "GROVE_CONFIG_HANDLER"
ENV_GROVE_SECRET_HANDLER = "GROVE_SECRET_HANDLER"
ENV_GROVE_TELEMETRY_URI = "GROVE_TELEMETRY_URI"
ENV_GROVE_WORKER_COUNT = "GROVE_WORKER_COUNT"
ENV_GROVE_LOCK_DURATION = "GROVE_LOCK_DURATION"
ENV_GROVE_CONFIG_REFRESH = "GROVE_CONFIG_REFRESH"

# Plugin groups (setuptools entrypoints).
PLUGIN_GROUP_CACHE = "grove.caches"
PLUGIN_GROUP_OUTPUT = "grove.outputs"
PLUGIN_GROUP_OBSERVER = "grove.observers"
PLUGIN_GROUP_CONFIG = "grove.configs"
PLUGIN_GROUP_PROCESSOR = "grove.processors"
PLUGIN_GROUP_SECRET = "grove.secrets"
PLUGIN_GROUP_CONNECTOR = "grove.connectors"

# Define defines for unset environment variables.
DEFAULT_CACHE_HANDLER = "local_memory"
DEFAULT_OUTPUT_HANDLER = "local_stdout"
DEFAULT_CONFIG_HANDLER = "local_file"

# Maximum number of connectors to execute concurrently.
DEFAULT_WORKER_COUNT = 50
DEFAULT_LOCK_DURATION = 300  # seconds.

# The default operation name to use where none is specified.
DEFAULT_OPERATION = "all"

# The default interval, in seconds, to refresh connector configuration from the backend.
DEFAULT_CONFIG_REFRESH = 300  # seconds.

# Run connectors every 10 minutes by default.
DEFAULT_CONFIG_FREQUENCY = 600  # seconds.

# The default multiple of a connector's frequency after which a collection is considered
# stale (i.e. overdue) by the observability layer. A value of 3 means a connector is
# flagged as stale once it has not completed for three times its configured frequency.
DEFAULT_STALENESS_FACTOR = 3

# The default number of consecutive zero-volume runs before an observer event is emitted.
# A value of 0 disables zero-volume alerting.
DEFAULT_ZERO_VOLUME_RUNS = 0

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove local daemon entrypoint."""

import datetime
import os
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

from aws_lambda_powertools import Logger

from grove.constants import (
    DEFAULT_CONFIG_REFRESH,
    DEFAULT_WORKER_COUNT,
    ENV_GROVE_CONFIG_REFRESH,
    ENV_GROVE_WORKER_COUNT,
)
from grove.entrypoints import base
from grove.exceptions import GroveException
from grove.logging import GroveFormatter


def runtime_information() -> Dict[str, str]:
    """Attempts to determine the runtime, returning the relevant runtime data.

    :return: A dictionary of runtime data.
    """
    # If Nomad, grab the relevant information.
    if os.environ.get("NOMAD_ALLOC_ID", None):
        return {
            "runtime_id": os.environ.get("NOMAD_ALLOC_ID", "NOT_FOUND"),
            "runtime_region": os.environ.get("NOMAD_REGION", "NOT_FOUND"),
            "runtime_job_name": os.environ.get("NOMAD_JOB_NAME", "NOT_FOUND"),
        }

    # If nothing else matched, assume a local process.
    return {
        "runtime_id": str(os.getpid()),
        "runtime_host": socket.gethostname(),
    }


def scheduler(queue: threading.Qu) -> None:
    """Defines a scheduler which handles submitting configuration to the worker pool."""


def entrypoint():
    """Provides the daemon entrypoint for Grove.

    This does not use the base entrypoint, as Grove in daemon mode operates slightly
    differently - as it needs to track the run state of connectors, and schedule new
    executions based on their last run-time.

    It may be possible to rationalise the two in future, but for now, we'll keep
    things separate.
    """
    context = {"runtime": __file__, **runtime_information()}
    logger = Logger(
        "grove",
        logger_formatter=GroveFormatter(context),
        stream=sys.stderr,
    )
    logger.info("Grove started")

    # Get the configuration refresh frequency.
    try:
        refresh_last = None
        refresh_frequency = int(
            os.environ.get(ENV_GROVE_CONFIG_REFRESH, DEFAULT_CONFIG_REFRESH)
        )
        logger.info(
            f"Configuration will be reloaded every {refresh_frequency} seconds."
        )
    except ValueError as err:
        logger.critical(
            f"Configuration refresh ('{ENV_GROVE_CONFIG_REFRESH}') must be a number.",
            extra={"exception": err},
        )
        return

    # Allow users to optionally configure the number of worker threads.
    try:
        workers = int(os.environ.get(ENV_GROVE_WORKER_COUNT, DEFAULT_WORKER_COUNT))
    except ValueError as err:
        logger.critical(
            f"Worker count ('{ENV_GROVE_WORKER_COUNT}') must be a number.",
            extra={"exception": err},
        )
        return

    # All connectors will be executed in their own thread, up to the maximum configured
    # workers specified by the worker count.
    logger.info("Spawning thread pool for connectors", extra={"workers": workers})

    while True:
        # (Re)load the configuration from the configured backend if required.
        if refresh_last:
            refresh_delta = (datetime.datetime.now() - refresh_last).seconds

        if not refresh_delta or refresh_delta >= refresh_frequency:
            try:
                configurations = base.configure()
                refresh_last = datetime.datetime.now()

                logger.info("Configuration has been refreshed from the backend.")
            except GroveException as err:
                # On failure to refresh, we could continue to run until the next refresh
                # is due in order to try and be fault tolerant. For now though, if we
                # fail to refresh, we'll bail. The run-time should reschedule us.
                logger.critical(
                    "Failed to load configuration from backend",
                    extra={"exception": err},
                )
                return

        # Yield between iterations.
        time.sleep(1)


# Support local development if called as a script.
if __name__ == "__main__":
    entrypoint()

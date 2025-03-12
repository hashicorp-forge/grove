# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove local daemon entrypoint."""

import datetime
import os
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor
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
from grove.models import Run


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
        refreshed_at = None
        since_refresh = None
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

    with ThreadPoolExecutor(max_workers=workers) as pool:
        runs: Dict[str, Run] = {}

        while True:
            if refreshed_at:
                since_refresh = datetime.datetime.now() - refreshed_at  # type:ignore

            # (Re)load the configuration from the configured backend if required.
            if not refreshed_at or since_refresh.seconds >= refresh_frequency:  # type: ignore
                try:
                    configurations = base.configure()
                    refreshed_at = datetime.datetime.now()

                    logger.info("Configuration has been refreshed from the backend.")
                except GroveException as err:
                    # On failure to refresh, we could continue to run until the next
                    # refresh is due in order to try and be fault tolerant. For now
                    # though, if we fail to refresh we'll bail as the run-time should
                    # reschedule us.
                    logger.critical(
                        "Failed to load configuration from backend",
                        extra={"exception": err},
                    )
                    return

            # On the first run of a execution we check if a run is due - which requires
            # a round-trip to the cache backend. For subsequent runs we check if we have
            # a local last dispatch time to try and avoid hitting the cache every time.
            for configuration in configurations:
                now = datetime.datetime.now(datetime.timezone.utc)
                ref = configuration.reference(suffix=configuration.operation)
                run = runs.get(ref, Run(configuration=configuration))

                # Use the frequency from the configuration, not the local object as it
                # may have been changed in the configuration.
                frequency = configuration.frequency

                if run.last is None or (now - run.last).seconds >= frequency:
                    # If there's a valid future on the local run object a run is still
                    # in progress.
                    if run.future is not None:
                        continue

                    # Otherwise, schedule it and track the run. If the connector isn't
                    # due to run, if it has run more recently in another location, then
                    # the local 'last' time will be replaced with the cached value when
                    # the future returns.
                    future = pool.submit(base.dispatch, configuration, context)
                    run.last = now
                    run.future = future
                    runs[ref] = run

            # TODO: Run objects for connectors which have their configuration documents
            # deleted aren't actually removed from the runs dictionary. These won't run
            # again, but should be cleaned up if removed from the configuration backend.

            # Check the status of all futures.
            for ref, run in runs.items():
                try:
                    if run.future is None or run.future.running():
                        continue

                    run.last = run.future.result()
                    run.future = None
                except GroveException as err:
                    logger.error(
                        "Connector exited abnormally.",
                        extra={
                            "exception": err,
                            "configuration": run.configuration.name,
                            "connector": run.configuration.connector,
                        },
                    )

                logger.info(
                    "Connector has exited.",
                    extra={
                        "configuration": run.configuration.name,
                        "connector": run.configuration.connector,
                    },
                )

            # Yield between iterations.
            time.sleep(1)


# Support local development if called as a script.
if __name__ == "__main__":
    entrypoint()

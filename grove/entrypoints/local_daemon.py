# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove local daemon entrypoint."""

import datetime
import logging
import os
import socket
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Queue
from typing import Dict

from grove.constants import (
    DEFAULT_CONFIG_REFRESH,
    DEFAULT_LOG_LEVEL,
    DEFAULT_WORKER_COUNT,
    ENV_GROVE_CONFIG_REFRESH,
    ENV_GROVE_LOG_LEVEL,
    ENV_GROVE_WORKER_COUNT,
    GROVE_LOGGER_ROOT,
)
from grove.entrypoints import base
from grove.exceptions import ConcurrencyException, GroveException
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


def initialise_worker(queue: Queue):
    """Initialise a Grove worker.

    Each configured connector is dispatched to a worker by the Grove daemon.
    """
    level = str(os.environ.get(ENV_GROVE_LOG_LEVEL, DEFAULT_LOG_LEVEL)).upper()
    logger = logging.getLogger(GROVE_LOGGER_ROOT)

    # Push all log messages into a queue, rather than attempting to emit directly.
    handler = QueueHandler(queue)
    logger.setLevel(level)
    logger.addHandler(handler)


def entrypoint():
    """Provides the daemon entrypoint for Grove.

    This does not use the base entrypoint, as Grove in daemon mode operates slightly
    differently - as it needs to track the run state of connectors, and schedule new
    executions based on their last run-time.

    It may be possible to rationalise the two in future, but for now, we'll keep
    things separate.
    """
    context = {
        "runtime": __file__,
        **runtime_information(),
    }

    # Setup a logging destination which will monitor a queue for log messages. This
    # allows for each connector, running as separate processes, to log to a common
    # destination.
    log_queue = Queue()
    log_handler = logging.StreamHandler(stream=sys.stderr)
    log_handler.setFormatter(GroveFormatter(context=context))

    # Setup the logging thread.
    log_thread = QueueListener(log_queue, log_handler)
    log_thread.start()

    # Setup a logger for the main thread.
    logger = logging.getLogger(GROVE_LOGGER_ROOT)
    logger.setLevel(str(os.environ.get(ENV_GROVE_LOG_LEVEL, DEFAULT_LOG_LEVEL)).upper())
    logger.addHandler(QueueHandler(log_queue))

    # Get the configuration refresh frequency.
    logger.info("Grove started")

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

    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=initialise_worker,
        initargs=(log_queue,),
    ) as pool:
        runs: Dict[str, Run] = {}
        while True:
            if refreshed_at:
                since_refresh = datetime.datetime.now() - refreshed_at  # type:ignore

            # (Re)load the configuration from the configured backend if required.
            if not refreshed_at or since_refresh.total_seconds() >= refresh_frequency:  # type: ignore
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

                if run.last is None or (now - run.last).total_seconds() >= frequency:
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
            completed = []

            for ref, run in runs.items():
                try:
                    if run.future is None or run.future.running():
                        continue

                    # Sync last run time from connector completion.
                    run.last = run.future.result()
                except ConcurrencyException:
                    # We don't consider concurrency to be abnormal - as it may indicate
                    # another worker has scheduled the connector before us.
                    pass
                except Exception as err:
                    # We catch as wide as exception as possible here to try and avoid
                    # an unhandled error in a connector from taking down the main event
                    # loop.
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

                # Track completed and failed connectors.
                completed.append(ref)

            # Clean-up completed runs.
            for complete in completed:
                candidate = runs.get(complete)
                candidate.future = None

            # Yield between iterations.
            time.sleep(0.25)


# Support local development if called as a script.
if __name__ == "__main__":
    entrypoint()

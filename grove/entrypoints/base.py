# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Provides functions used between entrypoints."""

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

from aws_lambda_powertools import Logger

from grove.constants import (
    DEFAULT_CONFIG_HANDLER,
    DEFAULT_WORKER_COUNT,
    ENV_GROVE_CONFIG_HANDLER,
    ENV_GROVE_SECRET_HANDLER,
    ENV_GROVE_WORKER_COUNT,
    PLUGIN_GROUP_CONFIG,
    PLUGIN_GROUP_CONNECTOR,
    PLUGIN_GROUP_SECRET,
)
from grove.exceptions import GroveException
from grove.helpers import plugin
from grove.logging import GroveFormatter
from grove.models import ConnectorConfig


def dispatch(config: ConnectorConfig, context: Dict[str, str]):
    """Executes a connector, blocking until complete.

    This function is intended to be called via a ThreadPoolExecutor to enable concurrent
    execution of connectors.

    :param config: A connector configuration object for this connector thread.
    :param context: Contextual information relating to the current runtime.
    """
    handler = plugin.lookup_handler(config.connector, PLUGIN_GROUP_CONNECTOR).load()
    instance = handler(config, context)
    instance.run()


def configure() -> List[ConnectorConfig]:
    """Fetches all configuration documents and associated secrets."""
    configs = plugin.load_handler(
        os.environ.get(ENV_GROVE_CONFIG_HANDLER, DEFAULT_CONFIG_HANDLER),
        PLUGIN_GROUP_CONFIG,
    )

    # Immediately ignore configuration documents if they're marked as disabled.
    loaded = []

    for configuration in configs.get():
        if configuration.disabled:
            continue

        loaded.append(configuration)

    # Secret backends are optional, so if there isn't one defined, assume secrets are
    # embedded in the configuration.
    handler = os.environ.get(ENV_GROVE_SECRET_HANDLER)

    if not handler:
        return loaded

    # Load all required secrets into the configuration objects from the configured
    # secrets backend.
    secrets = plugin.load_handler(handler, PLUGIN_GROUP_SECRET)
    return secrets.load(loaded)


def entrypoint(context: Dict[str, Any]):
    """Provides the main entrypoint for Grove.

    This function should be called from various wrappers in order to execute Grove when
    running under the respective runtime. This is in order to enable use in serverless,
    containerised, virtualised, and "bare metal" environments.

    :param context: Contextual information relating to the current runtime.
    """
    logger = Logger(
        "grove",
        logger_formatter=GroveFormatter(context),
        stream=sys.stderr,
    )
    logger.info("Grove started")

    # Attempt to load connector configuration, failure to do so is not recoverable.
    try:
        configurations = configure()
    except GroveException as err:
        logger.critical(
            "Failed to initialise configuration handler", extra={"exception": err}
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
        futures = {}

        for configuration in configurations:
            future = pool.submit(dispatch, configuration, context)
            futures[future] = configuration

        # Blocks until all threads have exited.
        for future in as_completed(futures):
            configuration = futures[future]
            del futures[future]

            # Last ditch effort to catch any unhandled exceptions to ensure that they're
            # logged out.
            try:
                future.result()
            except GroveException as err:
                logger.error(
                    "Connector exited abnormally.",
                    extra={
                        "exception": err,
                        "configuration": configuration.name,
                        "connector": configuration.connector,
                    },
                )

            # Exit logs are recorded on error / exception and success.
            logger.info(
                "Connector has exited.",
                extra={
                    "configuration": configuration.name,
                    "connector": configuration.connector,
                },
            )

    logger.info("All connectors have exited. Grove execution has finished.")

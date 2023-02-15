# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove local process entrypoint."""

import os
import socket
from typing import Dict

from grove.entrypoints import base


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
    """Grove local process entrypoint."""
    base.entrypoint(context={"runtime": __file__, **runtime_information()})


# Support local development if called as a script.
if __name__ == "__main__":
    entrypoint()

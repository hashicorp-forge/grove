#!/usr/bin/env python3
# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

from cookiecutter.main import cookiecutter

cookiecutter(
    ".",
    no_input=True,
    extra_context={
        "project_type": "outputs",
        "provider_name": "remote",
        "provider_product": "http",
    },
)

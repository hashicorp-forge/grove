# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

""" Provides post generation hooks to handle optional files.

This is required to ensure that files not relevant for the given project type are
properly removed.
"""

import os
import shutil


def main():
    # Move files around based on the selected project type.
    os.rename(
        "./{{cookiecutter.project_slug}}/inits/{{ cookiecutter.project_type }}.py",
        "./{{cookiecutter.project_slug}}/__init__.py",
    )

    # Remove files only used by specific project types if not required.
    if "{{ cookiecutter.project_type }}" != "connectors":
        os.remove("./{{cookiecutter.project_slug}}/example_logs.py")

    # Remove the handlers directory, as this is only used
    shutil.rmtree("./{{cookiecutter.project_slug}}/inits/")


if __name__ == "__main__":
    main()

""" Provides post generation hooks to handle optional files.

This is required to ensure that files not relevant for the given project type are
properly removed.
"""

import os
import shutil
import subprocess
import sys


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

    # Initialize a new git repository in the project, and push an initial commit.
    try:
        subprocess.run(["git", "init"], check=True)
        subprocess.run(["git", "checkout", "-b", "main"], check=True)
        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(
            [
                "git",
                "commit",
                "-m",
                "'Initial commit of Cookiecutter generated plugin.'",
            ],
            check=True,
        )
    except subprocess.CalledProcessError as err:
        print(f"ERROR: Unable to create git repository: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()

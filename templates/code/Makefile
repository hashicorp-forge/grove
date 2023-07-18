SHELL := /usr/bin/env bash -euo pipefail -c

# This make target is intended for validation of the CookieCutter as part of the Grove
# release process.
.PHONY: validate
validate:
	rm -rf grove-outputs-remote-http/
	python3 scripts/run_cookiecutter.py
	cd grove-outputs-remote-http && tox

SHELL := /usr/bin/env bash -euo pipefail -c

.PHONY: validate
validate:
	rm -rf grove-outputs-remote-http/
	python3 scripts/run_cookiecutter.py
	cd grove-outputs-remote-http && tox


MODULE_NAME = grove
SHELL := /usr/bin/env bash -euo pipefail -c

.PHONY: lint
lint:
	tox -e linters

.PHONY: test
test:
	tox

.PHONY: documentation
documentation:
	rm -rf docs/grove.*.rst
	sphinx-apidoc --module-first -q -o docs/ $(MODULE_NAME)
	sphinx-build -b html docs/ docs/_generated/

.PHONY: clean
clean:
	rm -rf \
		.coverage \
		.eggs \
		.mypy_cache \
		.pytest_cache \
		.tox \
		build \
		htmlcov \
		package \
		test-reports \
		$(MODULE_NAME).egg-info

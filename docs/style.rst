.. _PEP8: https://peps.python.org/pep-0008/
.. _Flake8: https://flake8.pycqa.org/en/latest/
.. _Tox: https://tox.wiki/en/latest/
.. _Black: https://black.readthedocs.io/en/stable/
.. _isort: https://pycqa.github.io/isort/
.. _PEP484: https://peps.python.org/pep-0484/

Style
=====

Grove ships with several tool configuration(s) which are used to enforce consistency.

Various linters are run during linting (via `Tox`_) and any errors in output should be
resolved before raising a pull-request.

To assist with this, "auto-formatters" are configured for Visual Studio Code. If using
VSCode to develop Grove, the :code:`.vscode/settings.json` configuration in root of the
Grove repository will automatically tell VSCode to run auto-formatters on file save.

Auto-formatting
---------------

Although installed as part of the :code:`tests` extra, the following tools are used to
enforce style related concerns.

* Code should be auto-formatted using `Black`_.
* Code must adhere to `PEP8`_.
* Imports must be sorted and consolidated using `isort`_.

Validation against `PEP8`_ is performed using `flake8`_ as part of linting runs.

Documentation
-------------

Grove uses Sphinx for documentation generation. In order to keep generated API
documentation up to date, docstrings and type annotations must be used.

* :code:`reST` (reStructured Text) format must be used for docstrings.
* `PEP484`_ type annotations must be used.

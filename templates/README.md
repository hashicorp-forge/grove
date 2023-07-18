## Grove Templates

This directory contains a number of templates for both deploying Grove, and creating new
new connectors and plugins.

Deployment templates are provided to allow users to deploy Grove into their environment
and start collecting logs as quickly as possible. Plugin and connector templates are
provided to allow the community to add new functionality to Grove, such as collection
of logs from new sources, or outputting logs to new backends.

Connector and plugin templates are built using [Cookiecutter](https://github.com/cookiecutter/cookiecutter)
which are provided to speed-up the creation of new Python which provide support for new
services.

If newly created plugins are intended to be public, newly created plugins can be
published by their authors to PyPi directly. Plugins and connectors intended to be
contributed back to the Grove project directly - where possible - cannot be created
using this Cookiecutter.

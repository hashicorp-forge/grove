## Grove Code Templates

This folder contains a [Cookiecutter](https://github.com/cookiecutter/cookiecutter)
template which provides a way to quickly generate new plugins and connectors.

To assist with consistency, this Cookiecutter adheres to the Grove style guidelines.
However, as third-party plugins and connectors aren't maintained by the Grove developers
this can be modified or removed if a different style is preferred by the author.

To assist with publishing, this Cookiecutter contains a set of Github Actions workflows
which will perform testing and linting on pull-requests, and can be used to publish
to PyPI on creation of a new Github release.

If these are not required, these files can be removed from the generated project.

### Questions

A number of questions will be asked during the Cookiecutter run to ensure the correct
template is setup. The majority of the values will be automatically generated based on
the initial answers.

However, for consistency, the following should be followed where possible:

* `provider_name`
  * If the plugin is for a protocol, rather than product, a `provider_name` like `local`
    or `remote` is preferred.
* `provider_product`
  * If the plugin is for a protocol, rather than a product, the `provider_product`
    should reference the protocol name - such as `http`.

### Usage

To generate a new connector or plugin, ensure Cookiecutter is installed, and then run
the following command from this folder.

```shell
cookiecutter ./
```

### Dependencies

In order to work with this template the following dependencies should be installed in
your development environment:

* [Python >= 3.9](https://www.python.org/downloads/release/python-390/)
* [`cookiecutter`](https://github.com/cookiecutter/cookiecutter)

Once Python is installed `cookiecutter` may be installed using `pip`:

```shell
pip install cookiecutter
```

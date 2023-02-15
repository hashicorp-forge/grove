# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath("../grove/"))

# Set defaults, and then load __about__ from the project.
__title__ = None
__about__ = None
__author__ = None
__version__ = None
__copyright__ = None

exec(open(os.path.abspath("../grove/__about__.py")).read())


# -- Project information -----------------------------------------------------

author = __author__
project = __title__.title()
copyright = __copyright__

# The full version, including alpha/beta/rc tags
release = __version__

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.githubpages",
    "sphinx.ext.viewcode",
]

add_function_parentheses = False
add_module_names = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "furo"
# html_logo = "static/logo.png"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["static"]

html_css_files = [
    "custom.css",
]

html_theme_options = {
    "sidebar_hide_name": True,
    "source_repository": "https://github.com/hashicorp-forge/grove",
    "source_branch": "main",
    "source_directory": "docs/",
    # CSS.
    "light_css_variables": {
        "color-foreground-secondary": "#444",
        "admonition-font-size": "0.9rem",
        "admonition-title-font-size": "0.9rem",
    },
    "dark_css_variables": {
        "color-foreground-secondary": "#444",
        "admonition-font-size": "0.9rem",
        "admonition-title-font-size": "0.9rem",
    },
}

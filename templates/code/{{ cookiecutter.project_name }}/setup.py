"""Setup for {{cookiecutter.project_slug}}."""
import os

from setuptools import setup

# Use the contents of README.md for PyPI.
path = os.path.dirname(os.path.abspath(__file__))
long_description = open(os.path.join(path, "README.md")).read()

setup(
    name="{{ cookiecutter.project_name }}",
    url="{{ cookiecutter.repository }}",
    version="0.0.1",
    description="{{ cookiecutter.project_description }}",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=["{{ cookiecutter.project_slug }}"],
    # Pin to major versions to allow upgrades inside of compatibility guarantees.
    install_requires=[
        "grove>=1.0.0,<2.0",
    ],
    # Register the plugin / connector for Grove to use.
    entry_points={
        {% if cookiecutter.project_type == "connectors" -%}
        "grove.connectors": [
            "{{ cookiecutter.provider_name }}_{{ cookiecutter.provider_product }}_example_logs = {{ cookiecutter.project_slug }}.example_logs:Connector",
        ],
        {%- else %}
        "grove.{{ cookiecutter.project_type }}": [
            "{{ cookiecutter.provider_slug }} = {{ cookiecutter.project_slug }}:Handler",
        ],
        {%- endif %}
    },
)

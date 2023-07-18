"""{{ cookiecutter.project_description }}."""

import logging
from typing import Optional

from pydantic import BaseSettings, Field, ValidationError

from grove.configs import BaseConfig
from grove.exceptions import ConfigurationException
from grove.helpers import parsing


class Configuration(BaseSettings):
    """Defines environment variables used to configure the {{ cookiecutter.provider_name }} {{ cookiecutter.provider_product }} handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    required_setting: str = Field(
        default="thing",
        description="A required configuration value (if unset a default will be used)",
    )
    optional_setting: Optional[str] = Field(
        description="An optional configuration value."
    )

    class Config:
        """Allow environment variable override of configuration fields.

        This also enforce a prefix for all environment variables for this handler. As
        an example the field `required_setting` would be set using the environment
        variable `{{ cookiecutter.project_slug.upper() }}_REQUIRED_SETTING`.
        """

        env_prefix = "{{ cookiecutter.project_slug.upper() }}_"
        case_insensitive = True


class Handler(BaseConfig):
    """A Grove handler for {{ cookiecutter.provider_name }} {{ cookiecutter.provider_product }}."""

    def __init__(self):
        """Sets up access to {{ cookiecutter.provider_product }}

        :raises ConfigurationException: There was an issue with configuration.
        """
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = Configuration()
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

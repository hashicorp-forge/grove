"""{{ cookiecutter.project_description }}."""

from typing import Optional

from pydantic import Field

from grove.outputs import BaseOutput


class Handler(BaseOutput):
    """A Grove handler for {{ cookiecutter.provider_name }} {{ cookiecutter.provider_product }}."""

    class Configuration(BaseOutput.Configuration):
        """Defines environment variables used to configure the {{ cookiecutter.provider_name }} {{ cookiecutter.provider_product }} handler.

        This should also include any appropriate default values for fields which are not
        required.
        """

        required_setting: str = Field(
            default="thing",
            description="A required value (if unset a default will be used).",
        )
        optional_setting: Optional[str] = Field(
            description="An optional configuration value."
        )

        class Config:
            """Allow environment variable override of configuration fields.

            This also enforce a prefix for all environment variables for this handler.
            As an example the field `required_setting` would be set using the
            environment `{{ cookiecutter.project_slug.upper() }}_REQUIRED_SETTING`.
            """

            env_prefix = "{{ cookiecutter.project_slug.upper() }}_"
            case_insensitive = True

    def setup(self):
        """Set up access to output backend.

        :raises AccessException: There was an issue ...
        """
        pass

    def submit(
        self,
        data: bytes,
        connector: str,
        identity: str,
        operation: str,
        part: int = 0,
        kind: Optional[str] = ".json.gz",
        descriptor: Optional[str] = "logs",
    ):
        """Persists captured data to ...

        :param data: Log data to write.
        :param connector: Name of the connector which retrieved the data.
        :param identity: Identity the collected data was collect for.
        :param operation: Operation the collected logs are associated with.
        :param part: Number indicating which part of the same log stream this file
            contains data for. This is used to indicate that the logs are from the same
            collection, but have been broken into smaller files for downstream
            processing.
        :param kind: An optional file suffix to use for files written.
        :param descriptor: An optional descriptor of the log stream being written.

        :raises AccessException: An issue occurred when writing data.
        """
        pass

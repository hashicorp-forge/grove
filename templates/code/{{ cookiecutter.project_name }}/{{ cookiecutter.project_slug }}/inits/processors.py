"""{{ cookiecutter.project_description }}."""

from typing import Any, Dict, List

from pydantic import Extra

from grove.models import ProcessorConfig
from grove.outputs import BaseOutput


class Handler(BaseOutput):
    """A Grove handler for {{ cookiecutter.provider_name }} {{ cookiecutter.provider_product }}."""

    class Configuration(ProcessorConfig, extra=Extra.forbid):
        """Expresses the configuration and associated validators for the processor."""

        pass

    def process(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Performs a set of processing operations.

        :param entry: A collected log entry.

        :return: The processed log entry with fields mapped, as a list.
        """
        return [entry]

# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove HashiCorp Vault secret handler."""

import logging
import urllib.parse
from typing import Optional, Tuple

import jmespath
import requests
from pydantic import BaseSettings, Field, ValidationError

from grove.exceptions import AccessException, ConfigurationException
from grove.helpers import parsing
from grove.secrets import BaseSecret


class Configuration(BaseSettings):
    """Defines environment variables used to configure the HashiCorp Vault handler.

    This should also include any appropriate default values for fields which are not
    required.
    """

    addr: str = Field(
        description="The address of the Vault instance to retrieve secrets from.",
    )
    token: Optional[str] = Field(
        description="An optional vault token to use when authenticating with Vault.",
        default=None,
    )
    token_file: Optional[str] = Field(
        description="An optional file to read the Vault token from.",
        default=None,
    )
    namespace: Optional[str] = Field(
        description="An optional Vault namespace that should be used.",
        default=None,
    )
    api_version: str = Field(
        description="An optional Vault API version to use (default: v1).",
        default="v1",
    )

    class Config:
        """Allow environment variable override of configuration fields.

        This also enforce a prefix for all environment variables for this handler. As
        an example the field `token` would be set using the environment variable
        `GROVE_SECRET_HASHICORP_VAULT_TOKEN`.
        """

        env_prefix = "GROVE_SECRET_HASHICORP_VAULT_"
        case_insensitive = True


class Handler(BaseSecret):
    def __init__(self):
        """Sets up access to Vault.

        This backend performs a pre-flight to validate that the configured token is
        able to be used to query vault.

        :raises ConfigurationException: There was an issue with configuration.
        :raises AccessException: An issue occurred attempting to access Vault.
        """
        self.logger = logging.getLogger(__name__)

        # Wrap validation errors to keep them in the Grove exception hierarchy.
        try:
            self.config = Configuration()  # type: ignore
        except ValidationError as err:
            raise ConfigurationException(parsing.validation_error(err))

        # If a token file is set any value passed as a token will be overwritten with
        # the value from file.
        if self.config.token_file:
            try:
                with open(self.config.token_file, "r") as fin:
                    self.config.token = fin.readline().strip()
            except OSError as err:
                raise ConfigurationException(
                    "Secrets handler could not read Vault token the configured token "
                    f"file of {self.config.token_file}: {err}"
                )

        # 'None' values will be automatically removed removed by requests when an HTTP
        # call is performed.
        self._headers = {
            "X-Vault-Token": self.config.token,
            "X-Vault-Request": "true",
            "X-Vault-Namespace": self.config.namespace,
        }

        # Perform a quick pre-flight to validate whether the credentials are valid.
        self._url = "/".join(
            [
                self.config.addr.rstrip("/"),
                self.config.api_version,
            ]
        )

        try:
            response = requests.get(
                f"{self._url}/auth/token/lookup-self",
                headers=self._headers,  # type: ignore
                allow_redirects=False,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            raise AccessException(
                f"Secrets handler could not access Vault at {self.config.addr}: {err}"
            )

    def get_field_and_path(self, path: str) -> Tuple[str, str]:
        """Extracts and removes 'field' parameters from a provided secret path.

        :param path: The path from the connector configuration to process.

        :raises VaultError: An error occurred while parsing data from the path.

        :returns: A tuple containing an extracted field, if any, and a Vault API
            compatible path.
        """
        url = urllib.parse.urlparse(path)
        qs = urllib.parse.parse_qs(url.query)

        # Extract and remove the field from the query parameters - if present.
        try:
            field = qs.pop("field", [])[0]
        except IndexError:
            raise ValueError("No 'field' parameter was found in the secret path.")

        # Regenerate the URL without the removed parameter, removing other parameters
        # we do not want configurable via the path.
        url = url._replace(netloc="", scheme="", params="")
        url = url._replace(query=urllib.parse.urlencode(qs, doseq=True))

        return field, urllib.parse.urlunparse(url).lstrip("/")

    def get(self, id: str) -> str:
        """Gets and returns a secret from Vault.

        To allow accessing different values under a configured secret path, this method
        uses a non-standard convention to encode which "field" of a returned credential
        is desired. This mimics the behavior of the Vault CLI "-field" option - though
        this is not a supported HTTP parameter by the Vault API directly.

        As an example of this, the following path would provide access to the 'password'
        portion of a credential stored in a KVv2 engine mounted at 'secret/':

            secret/data/example/demo?field=password

        To instead access a 'token' portion of a credential stored in the same path, the
        following would be used:

            secret/data/example/demo?field=token

        Finally, to perform the same operation against a KVv1 engine mounted at 'kv/'
        the path is almost the same. However, the '/data/' must ALSO be dropped, as this
        is only required for KVv2:

            kv/example/demo?field=token

        :param id: The path of the secret to retrieve - including engine.
        :param name: The name of the secret, defined by the connector configuration. If
            a 'field' is specified in the secret path this parameter will be ignored.

        :raises AccessException: An issue occurred when getting the secret from Vault.

        :returns: The plain-text secret from vault.
        """
        try:
            field, path = self.get_field_and_path(id)
        except ValueError as err:
            raise AccessException(
                f"Secrets handler could parse the provided Vault path of {id}. {err}"
            )

        try:
            response = requests.get(
                f"{self._url}/{path}",
                headers=self._headers,  # type: ignore
                allow_redirects=False,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            raise AccessException(
                f"Secrets handler failed to read secret from Vault path {id}. {err}"
            )

        # Return the first string value which is located using the desired name. This is
        # intended to return the first result without consideration of the engine.
        paths = [f"data.{field}", f"data.data.{field}"]
        secrets = response.json()

        for candidate in paths:
            secret = jmespath.search(candidate, secrets)
            if isinstance(secret, str):
                return secret

        raise AccessException(
            f"Secrets handler could not get field {field} from Vault path {path}"
        )

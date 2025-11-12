# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Base SalesForce connector for Grove."""

import time
from datetime import datetime
from typing import Any, Dict, Tuple

import requests
from simple_salesforce import Salesforce, SalesforceLogin
from simple_salesforce.exceptions import SalesforceError

from grove.connectors import BaseConnector
from grove.exceptions import ConfigurationException, RequestFailedException

# Salesforce API version
SF_VERSION = "51.0"
SF_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# Default OAuth 2.0 endpoint for Salesforce (fallback only)
SF_OAUTH_TOKEN_URL_DEFAULT = "https://login.salesforce.com/services/oauth2/token"


def parse_salesforce_timestamp(timestamp_str: str) -> datetime:
    """Parse Salesforce timestamp, handling both Z and timezone offset formats.

    Salesforce returns timestamps like:
    - 2025-09-16T12:34:56.000Z (with Z)
    - 2025-09-16T12:34:56.000+00:00 (with timezone offset)

    :param timestamp_str: The timestamp string from Salesforce
    :return: Parsed datetime object
    :raises ValueError: If timestamp cannot be parsed
    """
    if timestamp_str.endswith("Z"):
        # Normalize 'Z' to '+00:00' so %z works
        timestamp_str = timestamp_str[:-1] + "+00:00"

    return datetime.strptime(timestamp_str, SF_TIMESTAMP_FORMAT)


class BaseSalesforceConnector(BaseConnector):
    """Base class for Salesforce connectors with shared authentication and query logic."""

    def __init__(self, config: Any, context: Dict[str, Any]) -> None:
        """Initialize the connector with a configuration and context.

        :param config: Configuration options from the connector configuration file.
        :param context: Context about the connector's execution environment.
        """
        super().__init__(config, context)

        # Store configuration attributes that the base class expects
        self.key = getattr(self.configuration, "key", None) or ""
        self.identity = getattr(self.configuration, "identity", None) or ""

        # Rate limiting configuration (with defaults)
        self.max_retries = getattr(self.configuration, "max_retries", 3)
        self.retry_delay = getattr(self.configuration, "retry_delay", 1)

    @property
    def client_id(self):
        """Fetches the Salesforce client ID from the configuration.

        This is required for OAuth 2.0 client credentials flow.

        :return: The "client_id" portion of the connector's configuration.
        """
        try:
            return self.configuration.client_id
        except AttributeError:
            return None

    @property
    def client_secret(self):
        """Fetches the Salesforce client secret from the configuration.

        This is required for OAuth 2.0 client credentials flow.

        :return: The "client_secret" portion of the connector's configuration.
        """
        try:
            return self.configuration.client_secret
        except AttributeError:
            return None

    @property
    def instance_url(self):
        """Fetches the Salesforce instance URL from the configuration.

        This is the Salesforce instance URL where the API calls will be made.

        :return: The "instance_url" portion of the connector's configuration.
        """
        try:
            return self.configuration.instance_url
        except AttributeError:
            return None

    @property
    def token(self):
        """Fetches the SalesForce security token from the configuration.

        This is required for traditional username/password authentication.

        :return: The "token" portion of the connector's configuration.
        """
        try:
            return self.configuration.token
        except AttributeError:
            return None

    def _is_oauth_configured(self) -> bool:
        """Determines if OAuth 2.0 credentials are configured.

        :return: True if OAuth credentials are present, False otherwise.
        """
        return bool(self.client_id and self.client_secret)

    def _is_legacy_configured(self) -> bool:
        """Determines if legacy username/password credentials are configured.

        :return: True if legacy credentials are present, False otherwise.
        """
        return bool(self.key and self.identity and self.token)

    def _get_oauth_token_url(self) -> str:
        """Determines the OAuth token URL based on the instance URL.

        Uses the instance URL to construct the OAuth endpoint, falling back to
        the default production endpoint if no instance URL is provided.

        :return: The OAuth token URL for the Salesforce environment.
        """
        if not self.instance_url:
            # If no instance URL is provided, use default production endpoint
            return SF_OAUTH_TOKEN_URL_DEFAULT

        # Extract the base domain from the instance URL and construct OAuth endpoint
        # e.g., https://company.my.salesforce.com -> https://company.my.salesforce.com/services/oauth2/token
        instance_url = self.instance_url.rstrip("/")
        oauth_url = f"{instance_url}/services/oauth2/token"

        return oauth_url

    def get_oauth_access_token(self) -> Tuple[str, str]:
        """Obtains an access token using OAuth 2.0 client credentials flow.

        This method authenticates with Salesforce using the client credentials flow
        and returns an access token for API calls.

        :return: A tuple containing (access_token, instance_url)
        :raises ConfigurationException: If required configuration is missing
        :raises RequestFailedException: If authentication fails
        """
        if not self.client_id:
            raise ConfigurationException(
                "client_id is required for OAuth 2.0 authentication"
            )
        if not self.client_secret:
            raise ConfigurationException(
                "client_secret is required for OAuth 2.0 authentication"
            )

        session = requests.session()
        oauth_token_url = self._get_oauth_token_url()

        self.logger.debug(
            f"Using OAuth token URL: {oauth_token_url} for instance: {self.instance_url}",
            extra=self.log_context,
        )

        self.logger.debug(
            f"OAuth credentials - Client ID: {self.client_id[:8]}... (length: {len(self.client_id) if self.client_id else 0}), "
            f"Client Secret: {'*' * 8}... (length: {len(self.client_secret) if self.client_secret else 0})",
            extra=self.log_context,
        )

        try:
            response = session.post(
                oauth_token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            response.raise_for_status()

            token_data = response.json()
            access_token = token_data.get("access_token")
            instance_url = token_data.get("instance_url") or self.instance_url

            if not access_token:
                raise RequestFailedException("No access token received from Salesforce")
            if not instance_url:
                raise RequestFailedException(
                    "No instance URL received from Salesforce; set configuration.instance_url"
                )

            return access_token, instance_url

        except requests.exceptions.HTTPError as err:
            # Enhanced error logging for OAuth failures
            try:
                error_response = err.response.json()
                error_details = error_response.get(
                    "error_description", error_response.get("error", "Unknown error")
                )
                self.logger.error(
                    f"OAuth authentication failed: {error_details}",
                    extra={
                        **self.log_context,
                        "oauth_url": oauth_token_url,
                        "client_id": (
                            self.client_id[:8] + "..." if self.client_id else None
                        ),
                        "response_status": err.response.status_code,
                        "response_body": error_response,
                    },
                )
            except (ValueError, KeyError):
                # If we can't parse the error response, log the raw response
                self.logger.error(
                    f"OAuth authentication failed with unparseable response: {err}",
                    extra={
                        **self.log_context,
                        "oauth_url": oauth_token_url,
                        "client_id": (
                            self.client_id[:8] + "..." if self.client_id else None
                        ),
                        "response_status": err.response.status_code,
                        "response_text": err.response.text,
                    },
                )

            raise RequestFailedException(
                f"Unable to authenticate with Salesforce using OAuth 2.0: {err}"
            )
        except requests.exceptions.RequestException as err:
            raise RequestFailedException(
                f"Unable to authenticate with Salesforce using OAuth 2.0: {err}"
            )

    def get_legacy_credentials(self) -> Tuple[str, str]:
        """Obtains credentials using traditional username/password authentication.

        This method authenticates with Salesforce using the legacy username/password
        flow and returns session credentials for API calls.

        :return: A tuple containing (session_id, instance_url)
        :raises ConfigurationException: If required configuration is missing
        :raises RequestFailedException: If authentication fails
        """
        if not self.key:
            raise ConfigurationException(
                "key (password) is required for legacy authentication"
            )
        if not self.identity:
            raise ConfigurationException(
                "identity (username) is required for legacy authentication"
            )
        if not self.token:
            raise ConfigurationException(
                "token (security token) is required for legacy authentication"
            )

        session = requests.session()
        try:
            (sf_session, sf_instance) = SalesforceLogin(
                session=session,
                username=self.identity,
                password=self.key,
                security_token=self.token,
            )
            return sf_session, sf_instance
        except (SalesforceError, requests.exceptions.RequestException) as err:
            raise RequestFailedException(
                f"Unable to authenticate with Salesforce using legacy authentication: {err}"
            )

    def _query_with_retry(self, client: Salesforce, soql_query: str) -> Dict[str, Any]:
        """Execute a SOQL query with rate limit handling and retry logic.

        :param client: Salesforce client instance
        :param soql_query: SOQL query to execute
        :return: Query results
        :raises RequestFailedException: If query fails after all retries
        """
        last_exception: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                return client.query_all(soql_query)
            except SalesforceError as err:
                last_exception = err
                error_code = getattr(err, "error_code", None)

                # Check if this is a rate limit error
                if error_code in ["REQUEST_LIMIT_EXCEEDED", "QUERY_TIMEOUT"] or (
                    hasattr(err, "response") and err.response.status_code == 503
                ):

                    if attempt < self.max_retries:
                        # Exponential backoff with jitter
                        delay = self.retry_delay * (2**attempt) + (time.time() % 1)
                        self.logger.warning(
                            f"Rate limit hit, retrying in {delay:.1f}s (attempt {attempt + 1}/{self.max_retries + 1})",
                            extra=self.log_context,
                        )
                        time.sleep(delay)
                        continue
                    else:
                        self.logger.error(
                            f"Rate limit exceeded after {self.max_retries + 1} attempts",
                            extra=self.log_context,
                        )
                        break
                else:
                    # Non-rate-limit error, don't retry - re-raise the original error
                    raise err
            except Exception as err:
                last_exception = err
                break

        raise RequestFailedException(
            f"Unable to query SalesForce after {self.max_retries + 1} attempts: {last_exception}"
        )


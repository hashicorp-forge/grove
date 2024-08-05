# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""FleetDM Vulnerability API client.
"""

import logging
import time
from typing import Dict, Optional

import requests

from grove.exceptions import RateLimitException, RequestFailedException
from grove.types import AuditLogEntries, HTTPResponse

API_BASE_URI = "https://panel.int.fleetdm.hashicorp.services"


class Client:
    def __init__(
        self,
        token: Optional[str] = None,
        retry: Optional[bool] = True,
        api_uri: Optional[str] =  None,
    ):
        """Setup a new FleetDM Vulnerability API client.

        :param token: FleetDM API Bearer token.
        :param retry: Automatically retry if recoverable errors are encountered, such as
            rate-limiting.
        """
        self.retry = retry
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

    def _get(
        self,
        url: str,
        params: Optional[Dict[str, Optional[str]]] = None,
    ) -> HTTPResponse:
        """A GET wrapper to handle retries for the caller.

        :param url: URL to perform the HTTP GET against.
        :param params: HTTP parameters to add to the request.

        :raises RateLimitException: A rate limit was encountered.
        :raises RequestFailedException: An HTTP request failed.

        :return: HTTP Response object containing the headers and body of a response.
        """
        while True:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as err:
                # Retry on rate-limit, but only if requested.
                if getattr(err.response, "status_code", None) == 429:
                    self.logger.warning("Rate-limit was exceeded during request")
                    if self.retry:
                        time.sleep(int(err.response.headers.get("Retry-After", "1")))
                        continue
                    else:
                        raise RateLimitException(err)

                raise RequestFailedException(err)

        return HTTPResponse(headers=response.headers, body=response.json())

    def get_hosts(
        self,
        cursor: Optional[str] = None,                                     # query 	Page number of the results to fetch.
        per_page: Optional[str] = "100",                                 # query 	Results per page.
        order_key: Optional[str] = None,                                    # query 	What to order results by. Can be any column in the hosts table.
        after: Optional[str] = None,                                        # query 	The value to get results after. This needs order_key defined, as that's the column that would be used. Note: Use page instead of after
        order_direction: Optional[str] = None,                              # query 	Requires order_key. The direction of the order given the order key. Options include 'asc' and 'desc'. Default is 'asc'.
        status: Optional[str] = None,                                       # query 	Indicates the status of the hosts to return. Can either be 'new', 'online', 'offline', 'mia' or 'missing'.
        query: Optional[str] = None,                                        # query 	Search          # query keywords. Searchable fields include hostname, hardware_serial, uuid, ipv4 and the hosts' email addresses (only searched if the          # query looks like an email address, i.e. contains an '@', no space, etc.).
        additional_info_filters: Optional[str] = None,                      # query 	A comma-delimited list of fields to include in each host's additional object.
        team_id: Optional[str] = None,                                  # query 	Available in Fleet Premium. Filters to only include hosts in the specified team. Use 0 to filter by hosts assigned to "No team".
        policy_id: Optional[str] = None,                                # query 	The ID of the policy to filter hosts by.
        policy_response: Optional[str] = None,                              # query 	Requires policy_id. Valid options are 'passing' or 'failing'.
        software_version_id: Optional[str] = None,                      # query 	The ID of the software version to filter hosts by.
        software_title_id: Optional[str] = None,                        # query 	The ID of the software title to filter hosts by.
        software_status: Optional[str] = None,                              # query 	The status of the software install to filter hosts by.
        os_version_id: Optional[str] = None,                            # query 	The ID of the operating system version to filter hosts by.
        os_name: Optional[str] = None,                                      # query 	The name of the operating system to filter hosts by. os_version must also be specified with os_name
        os_version: Optional[str] = None,                                   # query 	The version of the operating system to filter hosts by. os_name must also be specified with os_version
        vulnerability: Optional[str] = None,                                # query 	The cve to filter hosts by (including "cve-" prefix, case-insensitive).
        device_mapping: Optional[str] = None,                               # query 	Indicates whether device_mapping should be included for each host. See "Get host's Google Chrome profiles for more information about this feature.
        mdm_id: Optional[str] = None,                                   # query 	The ID of the mobile device management (MDM) solution to filter hosts by (that is, filter hosts that use a specific MDM provider and URL).
        mdm_name: Optional[str] = None,                                     # query 	The name of the mobile device management (MDM) solution to filter hosts by (that is, filter hosts that use a specific MDM provider).
        mdm_enrollment_status: Optional[str] = None,                        # query 	The mobile device management (MDM) enrollment status to filter hosts by. Valid options are 'manual', 'automatic', 'enrolled', 'pending', or 'unenrolled'.
        macos_settings: Optional[str] = None,                               # query 	Filters the hosts by the status of the mobile device management (MDM) profiles applied to hosts. Valid options are 'verified', 'verifying', 'pending', or 'failed'. Note: If this filter is used in Fleet Premium without a team ID filter, the results include only hosts that are not assigned to any team.
        munki_issue_id: Optional[str] = None,                           # query 	The ID of the munki issue (a Munki-reported error or warning message) to filter hosts by (that is, filter hosts that are affected by that corresponding error or warning message).
        low_disk_space: Optional[str] = None,                           # query 	Available in Fleet Premium. Filters the hosts to only include hosts with less GB of disk space available than this value. Must be a number between 1-100.
        disable_failing_policies: Optional[str] = None,                 # query 	If true, hosts will return failing policies as 0 regardless of whether there are any that failed for the host. This is meant to be used when increased performance is needed in exchange for the extra information.
        macos_settings_disk_encryption: Optional[str] = None,               # query 	Filters the hosts by the status of the macOS disk encryption MDM profile on the host. Valid options are 'verified', 'verifying', 'action_required', 'enforcing', 'failed', or 'removing_enforcement'.
        bootstrap_package: Optional[str] = None,                            # query 	Available in Fleet Premium. Filters the hosts by the status of the MDM bootstrap package on the host. Valid options are 'installed', 'pending', or 'failed'.
        os_settings: Optional[str] = None,                                  # query 	Filters the hosts by the status of the operating system settings applied to the hosts. Valid options are 'verified', 'verifying', 'pending', or 'failed'. Note: If this filter is used in Fleet Premium without a team ID filter, the results include only hosts that are not assigned to any team.
        os_settings_disk_encryption: Optional[str] = None,                  # query 	Filters the hosts by the status of the disk encryption setting applied to the hosts. Valid options are 'verified', 'verifying', 'action_required', 'enforcing', 'failed', or 'removing_enforcement'. Note: If this filter is used in Fleet Premium without a team ID filter, the results include only hosts that are not assigned to any team.
        populate_software: Optional[str] = None,                        # query 	If true, the response will include a list of installed software for each host, including vulnerability data.
        populate_policies: Optional[str] = None,                        # query 	If true, the response will include policy data for each host.
    ) -> AuditLogEntries:
        """Fetches a list of audit logs which match the provided filters.
        :return: AuditLogEntries object containing a pagination cursor, and log entries.
        """
        # See psf/requests issue #2651 for why we can happily pass in None values and
        # not have the request key added to the URI.
        result = self._get(
            f"{API_BASE_URI}/api/v1/fleet/hosts",
            params={
                "page": cursor,
                "per_page": per_page,
                "order_key": order_key,
                "after": after,
                "order_direction": order_direction,
                "status": status,
                "query": query,
                "additional_info_filters": additional_info_filters,
                "team_id": team_id,
                "policy_id": policy_id,
                "policy_response": policy_response,
                "software_version_id": software_version_id,
                "software_title_id": software_title_id,
                "software_status": software_status,
                "os_version_id": os_version_id,
                "os_name": os_name,
                "os_version": os_version,
                "vulnerability": vulnerability,
                "device_mapping": device_mapping,
                "mdm_id": mdm_id,
                "mdm_name": mdm_name,
                "mdm_enrollment_status": mdm_enrollment_status,
                "macos_settings": macos_settings,
                "munki_issue_id": munki_issue_id,
                "low_disk_space": low_disk_space,
                "disable_failing_policies": disable_failing_policies,
                "macos_settings_disk_encryption": macos_settings_disk_encryption,
                "bootstrap_package": bootstrap_package,
                "os_settings": os_settings,
                "os_settings_disk_encryption": os_settings_disk_encryption, 
                "populate_software": populate_software,
                "populate_policies": populate_policies,            
            },
        )

        # FleetDM returns an empty hosts array if there's no more pages of results,
        # so swap this for None in this case to avoid having to rely on "falsy" conditions.
        cursor = result.body.get("hosts", {})
        if not cursor:
            cursor = None

        # Return the cursor and the results to allow the caller to page as required.
        return AuditLogEntries(cursor=cursor, entries=result.body.get("entries", []))

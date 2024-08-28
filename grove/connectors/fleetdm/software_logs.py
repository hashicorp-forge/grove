# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""FleetDM Vulnerability connector for Grove."""

from datetime import datetime, timedelta, timezone

from grove.connectors import BaseConnector
from grove.connectors.fleetdm.api import Client
from grove.exceptions import NotFoundException


class Connector(BaseConnector):
    NAME = "fleetdm_software_logs"
    POINTER_PATH = "updated_at"


    @property
    def jmespath_queries(self):
        """Fetches the parameters for the jmespath filters of the data response.

        Jmespath query language is defined and can be tested at https://jmespath.org/
        This allows you to configure what data to include or filter from the FleetDM response.

        An example is:
        "jmespath_queries": "{hostname:hostname,updated_at:updated_at,software_updated_at:software_updated_at,
        uuid:uuid,hardware_serial:hardware_serial,computer_name:computer_name,
        os_version:osversion,software:software[?vulnerabilities].{name:name,
        version:version,source:source,vulnerabilities:vulnerabilities[?cvss_score].{cve:cve,cvss_score:cvss_score,
        epss_probability:epss_probability,cisa_known_exploit:cisa_known_exploit,resolved_in_version:resolved_in_version}}}"

        This returns the following structure of data about each host:
        {
        updated_at:
        software_updated_at:
        hostname:
        uuid:
        hardware_serial:
        computer_name:
        os_version:
        software:{ (filtered to only software with vulnerabilities which have a CVSS score)
            name:
            version:
            source:
            vulnerabilities:{
                cve:
                cvss_score:
                epss_probability:
                cisa_known_exploit:
                resolved_in_version:
            }
        }

        :return: A string of the Jmespath response that should define the JSON object to return.
        Default is *, the full set of JSON response
        """
        try:
            p = self.configuration.jmespath_queries
        except AttributeError:
            return "*"

        return p

    @property
    def params(self):
        """Fetches the parameters for the API call.

        This is used to set what parameters to use in the API call.

        :return: The dict of params defined in the connector configuration.
        """
        try:
            p = self.configuration.params
        except AttributeError:
            return None

        return p
    
    @property
    def api_uri(self):
        """The URI for the API call
        For example: https://panel.fleetdm.example.com
        """
        try:
            p = self.configuration.api_uri
        except AttributeError:
            return None

        return p

    def collect(self):
        """Collects all software from the FleetDM API.
        """
        client = Client(
            token=self.key,
            params=self.params,
            jmespath_queries=self.jmespath_queries,
            api_uri=self.api_uri
            )
        

        # We do the API call for all software on each run as there's no way to filter
        # only software entries that have been changed. As a proxy to keep from loading
        # all software EVERY time, we use the counts_updated_at date which is updated
        # only when Fleet reprocesses what the current counts of installed software are,
        # which is run less frequently, and can be configured on the Fleet server.
        # Grove requires a pointer to be set, so the default pointer is set to 1 week ago
  
        try:
            pointer = datetime.fromisoformat(self.pointer)
        except NotFoundException:
            pointer = datetime.now(timezone.utc) - timedelta(days=7)
        self.pointer = str(pointer)
        cursor = 0

        # Page over data using the cursor, saving returned data page by page.
        while True:

            log = client.get_software(cursor=cursor,params=self.params,jmespath_queries=self.jmespath_queries,api_uri=self.api_uri,pointer=pointer)


            # Save this batch of log entries.
            self.save(log.entries)

            # Check if we need to continue paging.
            if log.cursor is None:
                break
            else:
                cursor = int(log.cursor)

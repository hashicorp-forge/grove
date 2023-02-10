# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Minimal setup for grove."""
import os

from setuptools import find_packages, setup

# These will be overwritten by the values from __about__.py
__version__ = "0.0.0"
__author__ = "Not Defined"

path = os.path.dirname(os.path.abspath(__file__))
exec(open(os.path.join(path, "grove/__about__.py")).read())  # noqa: S102

# Load the long description for PyPi.
long_description = open(os.path.join(path, "README.md")).read()

setup(
    name="grove",
    version=__version__,
    author=__author__,
    packages=find_packages(include=["grove", "grove.*"]),
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "console_scripts": [
            "grove = grove.entrypoints.local_process:entrypoint",
        ],
        "grove.entrypoints": [
            "aws_lambda = grove.entrypoints.aws_lambda:entrypoint",
            "local_process = grove.entrypoints.local_process:entrypoint",
        ],
        "grove.connectors": [
            "atlassian_audit_events = grove.connectors.atlassian.audit_events:Connector",
            "github_audit_log = grove.connectors.github.audit_log:Connector",
            "gsuite_activities = grove.connectors.gsuite.activities:Connector",
            "local_heartbeat = grove.connectors.local.heartbeat:Connector",
            "gsuite_alerts = grove.connectors.gsuite.alerts:Connector",
            "okta_system_log = grove.connectors.okta.system_log:Connector",
            "onepassword_events_itemusages = grove.connectors.onepassword.events_itemusages:Connector",  # noqa: B950
            "onepassword_events_signinattempts = grove.connectors.onepassword.events_signinattempts:Connector",  # noqa: B950
            "pagerduty_audit_records = grove.connectors.pagerduty.audit_records:Connector",
            "sf_event_log = grove.connectors.sf.event_log:Connector",
            "sfmc_audit_events = grove.connectors.sfmc.audit_events:Connector",
            "sfmc_security_events = grove.connectors.sfmc.security_events:Connector",
            "slack_audit_logs = grove.connectors.slack.audit_logs:Connector",
            "tfc_audit_trails = grove.connectors.tfc.audit_trails:Connector",
            "torq_activity_logs = grove.connectors.torq.activity_logs:Connector",
            "torq_audit_logs = grove.connectors.torq.audit_logs:Connector",
            "twilio_monitor_events = grove.connectors.twilio.monitor_events:Connector",
            "twilio_messages = grove.connectors.twilio.messages:Connector",
            "workday_activity_logging = grove.connectors.workday.activity_logging:Connector",
            "zoom_activities = grove.connectors.zoom.activities:Connector",
            "zoom_operationlogs = grove.connectors.zoom.operationlogs:Connector",
        ],
        "grove.caches": [
            "aws_dynamodb = grove.caches.aws_dynamodb:Handler",
            "local_memory = grove.caches.local_memory:Handler",
        ],
        "grove.outputs": [
            "aws_s3 = grove.outputs.aws_s3:Handler",
            "local_file = grove.outputs.local_file:Handler",
            "local_stdout = grove.outputs.local_stdout:Handler",
        ],
        "grove.configs": [
            "aws_ssm = grove.configs.aws_ssm:Handler",
            "local_file = grove.configs.local_file:Handler",
        ],
        "grove.secrets": [
            "aws_ssm = grove.secrets.aws_ssm:Handler",
        ],
    },
)

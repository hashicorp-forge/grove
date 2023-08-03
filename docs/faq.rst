.. _PyPI: https://pypi.org/
.. _Github: https://www.github.com

Frequently Asked Questions
==========================

"What is a plugin?"
^^^^^^^^^^^^^^^^^^^

**Plugins** tell Grove how to fetch connector configuration, where Grove should put
collected logs, how Grove should remember which logs have already been collected, and
where Grove can get secrets from.

Effectively, a Grove plugin implements support for a backend of a particular type.

An example of a Grove plugin is :meth:`grove.outputs.aws_s3`. This *output* plugin tells
Grove how to write collected logs to AWS' S3 service.

"What is a connector?"
^^^^^^^^^^^^^^^^^^^^^^

**Connectors**, tell Grove how to talk to particular application or service to collect
logs. Connectors are the main units of work executed by Grove, as each connector is
executed in an independent thread to allow collections to happen in parallel, and
prevent an issue with one application or service from affecting others.

An example of a Grove connector is :meth:`grove.connectors.github`. This connector tells
Grove how talk to Github to collect logs from the Github audit events API.

"Do I need to deploy Grove to a cloud provider?"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Grove does not need to be deployed to a cloud provider. Although Grove was originally
intended to collect logs periodically, Grove can support many different modes of
operation and deployment.

Grove can be deployed to a cloud provider, deployed in on-premises environments, and
even used "locally" as a command-line tool from a workstation in order to collect logs
as part of an specific investigation.

"Why is :code:`_grove` being added to my log entries?"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Grove automatically adds metadata to collected log entries. This information is intended
to provide provenance for collected log entries, including information about the version
of Grove which collected the log, and run-time information about where the collection
occurred.

As an example, the following :code:`_grove` metadata would be added to Github audit log
entries collected using Grove running from AWS Lambda:

.. code-block:: json

    {
        "connector": "grove.connectors.github.audit_log",
        "identity": "myorganisation",
        "operation": "web",
        "pointer": "1677153788430",
        "previous_pointer": "1677153598959",
        "collection_time": "2023-02-23T12:04:05Z",
        "runtime": {
            "runtime": "/var/task/grove/entrypoints/aws_lambda.py",
            "runtime_id": "1c8b0bee-ea7a-41e4-842d-aa273f38f353",
            "lambda_function_arn": "arn:aws:lambda:us-east-1:012345678912:function:grove",
            "lambda_function_memory_size": "1024",
            "lambda_request_id": "42d9a65e-8a56-4f5a-81b9-c193b468682c"
        },
        "version": "1.0.0rc1"
    }


"Why am I seeing duplicate logs?"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
    Grove and its connectors should always preference duplicate log entries over missed
    log entries.

The most common cause of duplicate logs are limitations in external applications and
services which Grove collects log entries from. A number of services unfortunately do
not support filtering of data at the same "granularity" as the data is returned.

As an example, if a service returns timestamps of audit events to the millisecond but
only allows filtering of events to the second, log entries may be missed between
collections if Grove were to ask for audit events that happened `after` the last seen
timestamp. However, if Grove were to ask for events that happened `on or after` the last
seen timestamp, the last collected audit events would be recollected - leading to
duplicates.

As a concrete example of this, if a previous Grove collection was completed after
collecting "Event 1" from the sample below, "Event 2" may be missed if a subsequent
Grove collection requested that all events AFTER :code:`2022-01-01 12:35:05` should be
returned.

.. code-block::

    2022-01-01 12:35:05.234 - Event 1 [Last record collected]
    2022-01-01 12:35:05.843 - Event 2 [Potentially missed]
    2022-01-01 12:35:06.422 - Event 3

It should be noted that although Grove does perform some deduplication where possible,
duplication of log entries is still possible under certain edge cases.

.. _pull-request:

"I built a new connector, can I open a pull-request?"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Grove may be able to accept pull-requests for merging new connectors into Grove.
However, the Grove project may not be able to accept pull-requests for connectors which
require licensing or paid subscription access.

If you are unsure, please open a Github issue and we'll let you know!

If you'd like to share a new connector or plugin directly with the community, please
publish this directly to `PyPI`_ or as a release in your source control management
(SCM) system - such as `Github`_.  Publishing extensions in this way allows users in the
community to :code:`pip install` this new extension and reference it as part their
deployment.

Please ensure to follow the naming conventions described in the provided templates and
the development section of this documentation to allow these to be discoverable.


"I built a new plugin, can I open a pull-request?"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Grove may be able to accept pull-requests for plugins which add or improve support
for new backends and processors. However, the Grove project may not be able to accept
plugins which have a significant number of required dependencies, or are for non-free
backends which we are unable to test or maintain (generally due to lack of licensing,
or access to the software).

If you are unsure, please open a Github issue and we'll let you know!

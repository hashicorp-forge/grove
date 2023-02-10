.. _PyPI: https://pypi.org/
.. _Github: https://www.github.com

Frequently Asked Questions
==========================

"What is a plugin?"
---------------------

**Plugins** tell Grove how to fetch connector configuration, where Grove should put
collected logs, how Grove should remember which logs have already been collected, and
where Grove can get secrets from.

Effectively, a Grove plugin implements support for a backend of a particular type.

An example of a Grove plugin is :meth:`grove.outputs.aws_s3`. This *output* plugin tells
Grove how to write collected logs to AWS' S3 service.

"What is a connector?"
------------------------

**Connectors**, tell Grove how to talk to particular application or service to collect
logs. Connectors are the main units of work executed by Grove, as each connector is
executed in an independent thread to allow collections to happen in parallel, and
prevent an issue with one application or service from affecting others.

An example of a Grove connector is :meth:`grove.connectors.github`. This connector tells
Grove how talk to Github to collect logs from the Github audit events API.

"Why am I seeing duplicate logs?"
---------------------------------

.. note::
    Grove and its connectors should always preference duplicate records over missed
    records.

The most common cause of duplicate logs are limitations in external applications and
services which Grove collects log data from. A number of services unfortunately do not
support filtering of data at the same "granularity" as the data is returned.

As an example, if a service returns timestamps of audit events down to the millisecond
but only allows filtering of events to the second, all events in that second would
need to be collected to try to prevent missed data later in the second.

In the example below, the previous Grove collection finished after collection of "Event
1". However, as the service only supports filtering to the second, a filter that says
events AFTER `2022-01-01 12:35:05` should be returned may result in "Event 2" being
missed.

.. code-block::

    2022-01-01 12:35:05.234 - Event 1 [Last record collected]
    2022-01-01 12:35:05.843 - Event 2

This issue may be solved in future by performing per event / record deduplication in
Grove, but this is not currently supported.

.. _pull-request:

"I built a new plugin, can I open a pull-request?"
-----------------------------------------------------

First off, thank you for helping to extend and make Grove better!

Unfortunately, Grove is not able to accept pull-requests for merging new connectors and
plugins into Grove directly. If you'd like to share a new connector or plugin with the
community, please publish this directly to `PyPI`_ or as a release in your source
control management (SCM) system - such as `Github`_.

Please ensure to follow the naming conventions described in the provided templates and
the development section of this documentation to allow these to be discoverable.

Publishing extensions in this way allows users in the community to :code:`pip install`
this new extension and reference it as part their deployment.


"I built a new plugin, can I open a pull-request?"
--------------------------------------------------

Please see :ref:`"I built a new plugin, can I open a pull-request?" <pull-request>`.
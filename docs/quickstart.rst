.. _AWS SSM Parameter Store: https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html
.. _pip: https://pypi.org/project/pip/
.. _Slack audit logs: https://api.slack.com/admins/audit-logs
.. _Slack documentation: https://api.slack.com/admins/audit-logs#install
.. _Docker: https://docs.docker.com/get-docker/
.. _virtual environment: https://docs.python.org/3/library/venv.html

Quick-start
===========

This section of the documentation provides an overview of how to get started with Grove.
This documentation aims to help new users deploy Grove and start collecting logs as
quickly as possible.

For users wanting to extend Grove, please see the :doc:`Internals <internals>` page.

For production deployment examples please see the :ref:`deployment <deployment>` page.

Docker
------

To run Grove for the first time using Docker, ensure `Docker`_ is installed and run:

.. code-block:: shell

    git clone https://github.com/hashicorp-forge/grove
    cd grove
    docker compose up

This will run Grove inside of a Docker container with an example connector configuration
document that generates "heartbeat" log messages every 5 seconds.

Installation
------------

.. note::
    It is recommended to install Grove into a Python `virtual environment`_.

Grove can be installed locally using `pip`_:

.. code-block:: shell

    pip install grove

Once installed, Grove can be run directly from the command-line with:

.. code-block:: shell

    grove

Please note, if attempting to run Grove without any configuration documents, Grove will
immediately raise an error and exit. These configuration documents are required as they
tell Grove which applications or services to talk to, and which credentials to use.

The following sections of this quick-start documentation provide examples of an initial
"heartbeat" run of Grove, and then how to perform a first collection of real logs.

Heartbeat
---------

The following commands provide an example of how to create an initial "heartbeat"
configuration document and run Grove locally.

This example uses the built-in :meth:`grove.connectors.local.heartbeat` connector,
which is designed for debugging and does not actually collect any logs. Instead, this
connector outputs a message every few seconds.

.. code-block:: shell

    # Create a configuration folder, and a heartbeat configuration.
    mkdir -p /tmp/grove
    cat > /tmp/grove/heartbeat.json << EOF
    {
        "name": "heartbeat",
        "identity": "heartbeat",
        "connector": "local_heartbeat",
        "key": ""
    }
    EOF

    # Tell Grove where to find configuration documents.
    export GROVE_CONFIG_LOCAL_FILE_PATH=/tmp/grove/

    # Run Grove.
    grove


At this point Grove is not actually collecting any logs, just outputting messages to
the terminal. The following section of this documentation details some important
information about how Grove works, and then how to perform your first real collection of
"real" logs.

Components
----------

In order for Grove to be as flexible as possible it is broken into several components.
Although how these components work is not required to use Grove, understanding what
these components are is important as they are referred to many times during deployment.

Grove has three main components: **Plugins**, **Connectors**, and **Pointers**.

A **Connector** is the Grove component which is responsible for defining how Grove talks
to an application or service in order to download logs. Connectors are configured using
connector configuration documents which are fetched from a configuration backend at
run-time.

An example of a Grove connector is :meth:`grove.connectors.github` which tells Grove how to
collect logs from Github.

It's worth nothing that some organisations may have multiple tenants or organisations
for a particular SaaS provider. To allow Grove to collect logs from all of these it
supports multiple *instances* of the same connectors to be configured.

.. note::
    **Take Away**: *Connectors* tell Grove how to get logs from an application or
    service.

A **Plugin** is the Grove component which is responsible for defining where Grove should
get connector configuration documents from, how Grove should output collected logs, how
Grove should remember which logs have already been collected, and where Grove can get
secrets from. Plugins are configured through environment variables.

An example of a Grove plugin is :meth:`grove.outputs.aws_s3`. This *output* plugin tells
Grove how to write collected logs to AWS' S3 service.

.. note::
    **Take Away**: *Plugins* provide *handlers* to talk to *backends*.

A **Pointer** is how Grove keeps track of log entries that it has already collected.
These pointers are stored in the configured cache backend - provided by a cache plugin -
to allow Grove to keep track of its place between collections.

By default, Grove uses the :code:`local_memory` cache backend, which only stores pointer
information in memory. This is great for "one-shot" and development use, but once Grove
exits this cache will be lost.

For production deployments a "real" cache backend **MUST** be used or Grove will collect
the same logs every time it runs.

.. note::
    **Take Away**: *Pointers* are used by Grove to keep track of what logs it has
    already collected.

Defaults
~~~~~~~~

In order for Grove to operate an **Output**, **Cache**, and **Configs** handler must be
configured (**Secret** backends are optional). To simplify deployment, these are set to
the following defaults:

* :code:`GROVE_OUTPUT_HANDLER` (*Default*: :code:`local_stdout`)
* :code:`GROVE_CONFIG_HANDLER` (*Default*: :code:`local_file`)
* :code:`GROVE_CACHE_HANDLER`  (*Default*: :code:`local_memory`)
* :code:`GROVE_SECRET_HANDLER` (*Default*: *None*)

.. note::
    Grove only allows a single plugin of a given type to be used at one time. For
    example, if Grove is configured to use the `AWS SSM Parameter Store`_ for
    configuration storage, all configuration documents **must** be stored in SSM.

First collection
----------------

This guide will configure Grove to perform the first collection of logs from Slack. In
addition to creating a new configuration document for the Slack connector, it will also
configure the output handler to write logs to disk.

If you are not using Slack, this guide can be modified to suit the application or
service desired by changing the :code:`connector` field of the configuration document,
and ensuring that any fields required by the connector are also set.

As no collection of logs from Slack has been performed before, there will be no
pointers saved in the cache. Due to this lack of pointers, the
:meth:`grove.connectors.slack` connector will collect the last week of logs.

To begin, an appropriate account and credential is required to be generated to allow
Grove to access Slack. This also requires a certain Slack subscription level to enable
the audit log APIs.

Please see the `Slack documentation`_ for more information about how to generate this
credential.

.. Warning::
    Some applications, require a high level of permissions to access audit and event
    logs. These credentials should be treated accordingly!

    In all cases where the application or service supports scoping, it is **strongly**
    recommended that generated tokens are scoped to only allow the appropriate
    :code:`read` permissions. This is in order to reduce the impact of a token used by
    Grove being stolen or leaked.

    Please see the application or service documentation of best practices around
    credential scoping.  If the vendor does not support this, we recommend opening a
    feature request with them to request better granularity of permissions related to
    log collection.

To begin, create a new directory which will be used to house both the connector
configuration documents as well as the collected logs.

.. code-block:: shell

    mkdir -p grove/config
    mkdir -p grove/output

Next, create the Slack configuration document to tell Grove to talk to Slack, making sure
to replace the value of the :code:`key`, :code:`name`, and :code:`identity` with the 
correct values for the deployment.

.. code-block:: shell

    cat > grove/config/heartbeat.json << EOF
    {
        "name": "EC0FFEE1",
        "identity": "EC0FFEE1",
        "connector": "slack_audit_logs",
        "key": "xoxb-..."
    }
    EOF

.. note::
    For production deployments it is recommended to store secrets in an appropriate
    credential vault, rather than directly in connector configuration documents.

    Please see the :ref:`secrets <secrets>` section of the configuration documentation
    for more information.

Now that Grove is configured, the first collection can be run by telling Grove which
backends to use, and starting Grove:

.. code-block:: shell

    # Tell Grove to output files to local files.
    export GROVE_OUTPUT_HANDLER="local_file"
    # Configure the configuration path.
    export GROVE_OUTPUT_LOCAL_FILE_PATH=${PWD}/grove/output/
    # Configure the output path.
    export GROVE_CONFIG_LOCAL_FILE_PATH=${PWD}/grove/config/
    
    # Start a Grove collection.
    grove

If credentials are valid and scoped appropriate, Grove should now begin collection of
data from Slack, outputting the collected logs into the :code:`./grove/output/`
directory.

.. warning::
    The default cache handler for Grove is :code:`local_memory`. As a result, once Grove
    exits, it will "forget" what data has already been collected.

    Although this is useful for "one-shot" and development environments, it should be
    replaced with an appropriate cache handler prior to production use!

Running Grove locally is not recommended for production use. To see how to deploy Grove
in a production configuration, please see the :ref:`deployment <deployment>` page.


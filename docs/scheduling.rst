Scheduling
==========

In most cases, for Grove to be effective, it must be configured to run on a particular
interval to ensure that new logs from configured sources are collected. This periodic
collection is enabled by one of Grove's runtime modes. The two modes currently provided
by Grove are:

1. Scheduled mode.
    * This is run by using the :code:`grove` command.
    * This executes all configured connectors once, and then exits.
    * This mode is intended to be used conjunction with an external scheduler, or to
      allow a single point-in-time collection of logs for investigation and incident
      response.
2. Daemon mode.
    * This is run using the :code:`groved` command.
    * This is a long running process which periodically executes all configured
      connectors at their configured :code:`frequency`.
    * This mode is intended to be run as a system service, or in a container runtime.

Scheduled mode
-------------

"Scheduled" mode is executed using the :code:`grove` command. 

Scheduled mode has no mode specific configuration option(s) which affect its runtime.

Daemon mode
-----------

Daemon mode is executed using the :code:`groved` command - rather than :code:`grove`.

In Daemon mode, Grove runs as a long-running process which executes connectors on their
configured frequency. This enables connectors to run until completion with no deadlines,
and allows each connector to be executed at a different frequency - which may be
important for certain types of connector which need to collect data more frequently than
others.

In daemon mode Grove has one important mode specific configuration option. As usual,
this is configurable using an environment variable using the same name.

* :code:`GROVE_CONFIG_REFRESH`
    * This option controls how frequently Grove will refresh these connector
      configuration documents from the configured backend.
    * Grove keeps a copy of all connector configuration documents in memory to prevent
      querying the configuration backend constantly in the event loop.
    * This allows connector configuration documents to be added, removed, and modified
      without the need to restart Grove.
    * This option defaults to 300 seconds.

.. Note::
    It is important to note that while connector configuration documents are kept in
    memory and periodically refreshed, secrets are fetched every time a connector is
    executed - if a secrets backend is also in use. This is done to enable the use of
    dynamic secrets engines, if supported by the configured secrets backend, and to
    allow for secrets to be rotated without Grove needing to be notified or updated.

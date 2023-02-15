Configuration
=============

Grove contains two main mechanisms for configuration:

* Environment variables for the configuration of Grove.
* Configuration documents for the configuration of connectors.

.. Note::
    Secrets may also be considered part of the Grove configuration. However, as these
    primarily relate to connectors, information about them is covered following the
    "connectors" section of this documentation.

Connectors
~~~~~~~~~~

As almost all APIs are created differently, Grove must provide a flexible way of
configuring connectors. This configuration needs a loose specification, as connectors
for one service may have a different set of configuration fields to another.

As an example, a connector to collect logs from on-premises deployment of Github
Enterprise would need the fully-qualified domain name (FQDN) of the deployed service to
be able to collect its logs. However, this additional configuration field may not be
required for another service which only provides their product in a SaaS model.

In order to enable this workflow, Grove configuration documents are expressed as JSON
documents which are loaded and validated by Grove at run-time. Although there are a set
of common and required fields, connector developers may arbitrarily add new fields to
support the fields required by an application or service.

Grove automatically fetches all connector configuration documents from the configuration
backend specified by the :code:`GROVE_CONFIG_HANDLER` environment variable (*Default*:
:code:`local_file`) at run-time.

Connector configuration plugins allow storage of connector configuration documents in
many different places, from local files on disk, to distributed data stores.

.. Note::
    If no appropriate configuration backend exists for an environment, a new plugin
    can be developed to support it.

Required Fields
^^^^^^^^^^^^^^^

As specified in :meth:`grove.models.ConnectorConfig`, the following fields are required
for a configuration document to be valid:

  * :code:`name`

    * A user provided string used to uniquely identify this connector configuration.

  * :code:`identity`

    * The identity portion of the credential used to authenticate with the service or
      application. This may be a username, a realm, etc.
    * If no identity is required, such as in the case of an API that uses a Bearer token
      for authentication, this **MUST** still be set. In this case, this may set to any
      unique value associated with the provider - such as enterprise identifier, account
      name, etc.

  * :code:`connector`

    * The name of the connector which this configuration document is for. This must
      match the :code:`NAME` value defined in the connector.
    * For example, setting this field to :code:`tfc_audit_trails` would instruct Grove
      to use the :meth:`grove.connectors.tfc.audit_trails` connector.

.. Warning::
    An additional field called :code:`key` **MUST** either be set to the secret
    component of the credential used to authenticate with the application or service,
    or a :code:`key` entry **MUST** be added to the :code:`secrets` field of the
    document.

    When a :code:`key` is not set directly in the configuration document, Grove assumes
    that the credential will be looked up from a configured secrets backend. In order
    for Grove to do this, it will look for the path / identifier to use when looking up
    the secret from the :code:`secrets` field.

    If either of these is not set, the connector will not load.

    See the :doc:`Examples <examples>` section of this documentation for examples.

A complete example of a valid configuration document has been included below. In this
example, audit logs are being collected from Slack using their Audit API. In this case
the bearer token would be passed to the connector as a :code:`key` and with the
:code:`identity` set to :code:`EC0FFEE1` - the Slack enterprise identifier associated
with the configured organisation.

.. code-block:: json

  {
    "identity": "EC0FFEE1",
    "key": "xoxb-...",
    "connector": "slack_audit",
    "name": "Slack-EC0FFEE1"
  }

If should be noted that there are several other built-in optional fields that may be
used to assist with encoding (:code:`encoding`), disabling a connector
(:code:`disabled`), specifying the location of secrets for a connector
(:code:`secrets`), allowing filtering of log data (:code:`operation`), and more.

Please see the :meth:`grove.models.ConnectorConfig` implementation for more details.

.. _secrets:

Secrets
~~~~~~~

In addition to non-sensitive configuration data, connectors also require access to
secrets in order to be able to interact with a service. Although Grove supports storage
of these secrets inside of a connector configuration directly, it may be desired to
instead query these secrets from a credential vault during runtime.

This enables a workflow where configuration may be stored in a system with different
security requirements than the associated secrets. This also allows for just-in-time
creation of temporary credentials for services which support this operation.

In order to enable these workflows, each connector configuration document has a field
named :code:`secrets`.

Entries in this :code:`secrets` field are expressed as key / value pairs, where the key
defines the name of the field to create with the retrieved secret, and the value
represents a path, identifier, or other information known to the configured secrets
backend.

As an example, if Grove was configured to use the :code:`aws_ssm` secrets plugin, the
following connector configuration document would instruct Grove to query AWS SSM for an
encrypted parameter with a path of :code:`/grove/secrets/slack/EC0FFEE1`. The value
returned from SSM would then be used in place of the field named :code:`key`.

.. code-block:: json

    {
        "name": "Slack-EC0FFEE1",
        "identity": "EC0FFEE1",
        "connector": "slack_audit",
        "secrets": {
            "key": "/grove/secrets/slack/EC0FFEE1"
        }
    }

The use of a secrets backend allows sensitive information to be removed from
configuration documents, allowing configuration documents to be stored directly on disk,
or in other less sensitive backends where non-administrative users may have read-access.

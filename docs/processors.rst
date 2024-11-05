Processors
===========

Processors provide an optional facility to allow transformation of collected log records
before output. Processors are defined as part of a connector configuration document, and
are able to be chained together in order to perform a particular set of operations in
sequence.

A full list of available processors can be found in the submodules section of the
:meth:`grove.processors` documentation.

.. note::
   Custom processors can be created in the same way as plugins. This can assist when
   performing specific processing of log records not already supported by the built-in
   Grove processors. For more information, please see the :doc:`internals` section of
   this documentation.

Configuration
^^^^^^^^^^^^^

Processors are configured in the `processors` list inside of a connector configuration
document. This list should contain each processor which is required to be run, in the
desired order.

Each processor requires, at a minimum, that a :code:`name` and a :code:`processor` field
are defined. However, each processor have their own set of configuration fields which
are used to define how the processor should operate on a log record.

To understand exactly which processor requires which fields, please refer to the
relevant :meth:`grove.processors` documentation.


Example
^^^^^^^

As an example of using processors together in order to transform collected log records,
the following example flattens Google Workspace activity logs, and ensures that there is
only one event per log record:

.. code-block:: json

    "processors": [
        {
            "name": "One event per log entry",
            "processor": "split_path",
            "source": "events"
        },
        {
            "name": "Flatten and zip event parameters",
            "processor": "zip_paths",
            "source": "events.parameters",
            "key": "name",
            "values": [
                "value",
                "intValue",
                "boolValue",
                "multiValue",
                "multiIntValue",
                "multiBoolValue"
            ]
        }
    ]

In this example, two processors are in use: `split_path`, and `zip_paths`.

In order to demonstrate the operations that these processors have on a log record, the
following section provides sample log records before and after processing by a given
processor.

split_path
~~~~~~~~~

Split path is useful for upstream services which aggregate multiple events into a single
log record. In these cases, a single log record returned by a service may have multiple
events within it - rather than event one per log record. This can result in complexity
when attempting to parse and index these records in downstream log platforms.

In order to handle this, the :code:`split_path` processor generates new log records for
each event, cloning the rest of the log record. As an example, the :code:`split_path`
processor configuration defined in the section above when working on the following log
record:

.. code-block:: json

    {
        "id": "00001",
        "events": [
            {
                "operation": "create",
                "parameters": [
                    {"name": "username", "value": "example"},
                    {"name": "ip", "value": "192.0.2.1"}
                ]
            },
            {
                "operation": "update",
                "parameters": [
                    {"name": "username", "value": "example"},
                    {"name": "ip", "value": "192.0.2.1"}
                ]
            }
        ]
    }

Would instead be output as two log records with the following structure:

.. code-block:: json

    {
        "id": "00001",
        "events": {
            "operation": "create",
            "parameters": [
                {"name": "username", "value": "example"},
                {"name": "ip", "value": "192.0.2.1"}
            ]
        }
    },
    {
        "id": "00001",
        "events": {
            "operation": "update",
            "parameters": [
                {"name": "username", "value": "example"},
                {"name": "ip", "value": "192.0.2.1"}
            ]
        }
    }

zip_paths
~~~~~~~~~

Continuing from the example configuration and log record above, Zip Paths can be used to
extract "generic" key / value pairs back into fields with their respective names.

As an example, the :code:`zip_paths` processor configuration defined in the section
above when working on the log records output from the :code:`spit_path` example above:

.. code-block:: json

    {
        "id": "00001",
        "events": {
            "operation": "create",
            "parameters": [
                {"name": "username", "value": "example"},
                {"name": "ip", "value": "192.0.2.1"}
            ]
        }
    },
    {
        "id": "00001",
        "events": {
            "operation": "update",
            "parameters": [
                {"name": "username", "value": "example"},
                {"name": "ip", "value": "192.0.2.1"}
            ]
        }
    }

Would output the following log records:

.. code-block:: json

    {
        "id": "00001",
        "events": {
            "operation": "create",
            "parameters": {
                "username": "example",
                "ip": "192.0.2.1"
            }
        }
    },
    {
        "id": "00001",
        "events": {
            "operation": "update",
            "parameters": {
                "username": "example",
                "ip": "192.0.2.1"
            }
        }
    }

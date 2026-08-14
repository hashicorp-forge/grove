.. _rfc_kafka_output:

Grove Kafka Output
==================

----

Grove's existing output handlers — S3, local file, and HTTP — operate on a
batch-and-deliver model. A connector completes a full API pagination cycle, serializes
the collected records to gzip-compressed NDJSON, and writes the result to the configured
destination. For S3, this means logs are available to downstream consumers only after a
poll cycle completes and the object lands — introducing structural latency of 2–10
minutes depending on collection interval and SaaS API response times. When downstream
consumers are themselves polling S3 on a schedule, the latency compounds further. For a
security observability programme, this turns Grove into a batch telemetry source rather
than a streaming one: sufficient for historical investigation, but ill-suited for
real-time detection. Threats that manifest as multi-step sequences across SaaS platforms
— an Okta authentication anomaly immediately followed by a GitHub repository export, or
a Salesforce bulk export followed by a Slack DM to an external recipient — require
cross-source correlation within a detection window measured in seconds, not minutes.
Shipping logs directly into Kafka eliminates the delivery component of this latency:
once a connector's poll cycle completes, records are in the broker and available to
consumers within milliseconds, bounding worst-case detection latency to Grove's
collection interval alone rather than the sum of collection, delivery, and consumer-poll
delays.

Kafka is the appropriate primitive for this integration for reasons beyond throughput.
Its core abstraction — a durable, partitioned, replayable append-only log — addresses
several operational constraints simultaneously. Multiple independent consumer groups
(SIEM, data lake, anomaly detection engine, alerting pipeline) can consume the same
topic concurrently, each maintaining its own committed offset, which means Grove makes
one SaaS API call per collection cycle regardless of how many downstream consumers
exist — preserving vendor rate-limit headroom and eliminating duplicated collection
infrastructure. Kafka's retention model also provides backpressure absorption: a
consumer that is unavailable for hours simply resumes from its last offset on recovery,
in contrast to HTTP-push outputs where consumer downtime within Grove's retry window
causes permanent data loss. Critically for an evolving detection programme, Kafka
retention enables replay: when a new detection rule is written, analysts can evaluate
it against historical stream data without re-polling SaaS APIs. On the implementation
side, the handler is registered as a setuptools entry-point under ``grove.outputs``
and selected via ``GROVE_OUTPUT=kafka`` at runtime, requiring no changes to any
existing connector. It overrides ``BaseOutput.serialize()`` to return plain NDJSON
rather than gzip-compressed bytes, delegating wire compression to ``librdkafka``
(default: ``lz4``). The ``submit()`` method splits the incoming payload into chunks
bounded by two independent thresholds — ``max_records_per_message`` (default 500) and
``max_bytes_per_message`` (default 750 KB, intentionally conservative against Kafka's
1 MiB ``max.message.bytes`` default to leave headroom for message headers) — producing
each chunk with a registered ``on_delivery`` callback. After all chunks are enqueued,
``producer.flush(timeout)`` blocks until the internal ``librdkafka`` queue drains. If
flush returns a non-zero undelivered count, or if any delivery callback recorded an
error, the handler raises ``AccessException``, which causes Grove's collection loop to
leave the cursor unadvanced and retry the full batch on the next run, giving at-least-once
delivery semantics compatible with Grove's stateless, poll-and-flush execution model.

ksqlDB closes the final gap between log availability and automated detection. Because
ksqlDB treats Kafka topics as first-class streams, it can run continuous SQL queries
directly against Grove's output topics with no additional ingestion step. Connectors
that set a ``descriptor`` field — Salesforce sets the event log type; other connectors
may set an operation subtype — are routed by the handler to a Kafka topic matching the
descriptor name, giving ksqlDB fine-grained source streams (``LoginEvent``,
``okta_system_log``, ``github_audit_log``) as query targets rather than requiring
filter predicates on a catch-all topic. The ``_grove`` metadata envelope injected into
every record — carrying ``connector``, ``identity``, ``operation``, and
``collected_at`` — provides the join and grouping keys for cross-source correlation
queries without parsing vendor-specific payload schemas. For outlier detection, ksqlDB's
windowed aggregations map directly onto the statistical patterns present in SaaS audit
logs: tumbling-window counts can identify spikes in API call rates, bulk data exports,
or authentication failures; session windows can detect credential usage sequences that
span multiple sources within a configurable gap period. As an example, the following
persistent query identifies identities whose GitHub API call rate in any 5-minute window
exceeds three times their computed 24-hour rolling average — a signal consistent with
automated credential abuse or bulk exfiltration:

.. code-block:: sql

    -- Materialise a 24-hour rolling baseline per identity.
    CREATE TABLE github_baseline AS
        SELECT
            EXTRACTJSONFIELD(value, '$._grove.identity') AS identity,
            COUNT(*) / 288.0 AS avg_calls_per_5min  -- 288 x 5-min windows per day
        FROM grove_github_audit_log
            WINDOW TUMBLING (SIZE 24 HOURS)
        GROUP BY EXTRACTJSONFIELD(value, '$._grove.identity');

    -- Emit an alert row when the current window exceeds 3x the baseline.
    CREATE TABLE github_access_anomalies AS
        SELECT
            w.identity,
            w.calls_in_window,
            w.calls_in_window / b.avg_calls_per_5min AS spike_ratio
        FROM (
            SELECT
                EXTRACTJSONFIELD(value, '$._grove.identity') AS identity,
                COUNT(*) AS calls_in_window
            FROM grove_github_audit_log
                WINDOW TUMBLING (SIZE 5 MINUTES)
            GROUP BY EXTRACTJSONFIELD(value, '$._grove.identity')
        ) w
        JOIN github_baseline b ON w.identity = b.identity
        WHERE w.calls_in_window / b.avg_calls_per_5min > 3.0
        EMIT CHANGES;

The result stream ``github_access_anomalies`` is itself a Kafka topic, allowing it to
feed directly into a SIEM, a PagerDuty connector, or a secondary ksqlDB rule that
correlates the spike with concurrent anomalies on other Grove source topics — composing
layered detection logic without a separate processing cluster or custom application code.

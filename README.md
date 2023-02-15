## Grove

> Grove is not an official HashiCorp project.

Grove is a Software as a Service (SaaS) log collection framework, designed to support
collection of logs from services which do not natively support log streaming.

Grove enables teams to collect security related events from their vendors in a reliable
and consistent way. This data may then be stored and analyzed with a team's _existing_
tooling in order to support threat detection and compliance programmes.

Out of the box, Grove provides:

* 🪵 Reliable and periodic log collection of logs.
* ☁️ Support a large number of widely used SaaS applications and services.
* 🧱 Plugin based "connectors" to enable support for new applications and services.
* 🧳 "Bring your own" caching, output, configuration, and secrets backends.

Grove was created and is currently maintained by the HashiCorp security team.

**Please note**: While this is not an official HashiCorp project, security is still very
important to us! If you have found a potential security issue with Grove, please contact
us via email at security@hashicorp.com, rather than filing a GitHub issue.

### Supported Sources

Currently the following log sources are supported by Grove out of the box. If a source
isn't listed here, support can be added by creating a custom connector!

* Atlassian audit events (e.g. Confluence, Jira)
* GitHub audit logs
* GSuite alerts
* GSuite activity logs
* Okta system logs
* 1Password sign-in attempt logs
* 1Password item usage event logs
* PagerDuty audit records
* SalesForce Cloud event logs
* SalesForce Marketing Cloud audit event logs
* SalesForce Marketing Cloud security event logs
* Slack audit logs
* Terraform Cloud audit trails
* Torq activity logs
* Torq audit logs
* Twilio monitor events
* Twilio message logs
* Workday activity logs
* Zoom activity logs
* Zoom operation logs

### Documentation

Please see the [Grove documentation](https://hashicorp-forge.github.io/grove/) for full
documentation, information about Grove's internals, and API information.

### Quick Start

To run Grove for the first time using [Docker](https://docs.docker.com/get-docker/),
ensure `docker` is installed and run:

```sh
git clone https://github.com/hashicorp-forge/grove
cd grove
docker compose up
```

This should see log messages from a "heartbeat" connector every 5 seconds. For more
detailed examples and information, please see the [Grove documentation](https://hashicorp-forge.github.io/grove/).

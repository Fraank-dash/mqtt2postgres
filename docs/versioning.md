# Versioning

Package versions are derived from Git tags via `setuptools-scm`.

## Baseline Diagram

The current repository baseline is:

```mermaid
flowchart TD
    Parent["mqtt2postgres<br/>v0.9.3<br/>cd0b4c9"]
    Broker["examples/local-stack/mosquitto<br/>mosquitto-broker<br/>v0.9.1-fork.1<br/>05c1b21"]
    Sql["examples/sql/mqtt-ingest<br/>mqtt-ingest-sql<br/>v0.9.2-fork2.0<br/>78d2725"]

    Parent --> Broker
    Parent --> Sql
```

Use release tags such as:

```text
v0.2.0
v0.2.1
```

In GitHub Actions, make sure checkout fetches tags:

```yaml
fetch-depth: 0
```

The `fallback_version` in `pyproject.toml` is only used when Git tag metadata is unavailable.

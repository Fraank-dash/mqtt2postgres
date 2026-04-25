# Changelog

## Unreleased

## 0.5.0 - 2026-04-25

### Topic: Database Ingest
- Added parsed `device_id` and `metric_name` dimensions for raw and aggregate sensor data shaped like `sensors/<device>/<metric>`.
- Changed the 3-minute aggregate refresh path to group and interpolate per device and metric instead of only by full topic.

### Topic: Testing
- Expanded the local smoke test to verify multi-device aggregation under `sensors/+/temp`.

### Topic: Publisher
- Added JSON-based publisher configuration support via `--config`, with one file able to define multiple publishers and multiple independently generated topics per publisher.
- Switched the local Docker publisher service to a read-only mounted JSON config instead of embedding topic and generator arguments in the Compose command.

### Topic: Subscriber
- Added JSON-based subscriber configuration support via `--config`, with one subscriber definition able to carry multiple topic filters.
- Switched the local Docker subscriber service to a read-only mounted JSON config instead of embedding broker, database, and topic-filter arguments in the Compose command.

## 0.4.0 - 2026-04-25

### Topic: Database Ingest
- Replaced table-specific MQTT routes with topic subscriptions that call a Postgres ingest function.
- Added a generic TimescaleDB hypertable and `mqtt_ingest.ingest_message` bootstrap function for raw MQTT message storage.
- Split the local TimescaleDB bootstrap SQL into ordered schema, table, function, and job files.
- Added stored 3-minute topic aggregates with `tba` and `aggregated` status values.
- Added bucket-boundary LOCF and linear interpolation fields plus boundary-aware time-weighted averages for stored 3-minute aggregates.
- Added support for plain numeric publisher payloads alongside traced JSON payloads.

### Topic: Local Stack
- Switched the local TimescaleDB image to `timescale/timescaledb-ha:pg16` so `timescaledb_toolkit` is available during bootstrap.
- Added a dedicated helper script for inspecting stored 3-minute aggregate rows.

### Topic: Configuration
- Replaced `--route TOPIC_FILTER=TABLE` with repeated `--topic-filter` flags plus `--db-ingest-function`.
- Added `MQTT2POSTGRES_DB_INGEST_FUNCTION` as an environment fallback for the ingest function name.

### Topic: Testing
- Updated the smoke test and unit suite for ingest-function writes and stored aggregate behavior.

## 0.3.0 - 2026-04-25

### Topic: Local Testing
- Added a four-container local Compose stack with continuous publisher, broker, subscriber, and TimescaleDB services.
- Added a local Mosquitto + TimescaleDB 16 compose stack and wrapper scripts for quick MQTT development testing.
- Converted the local example target tables to TimescaleDB hypertables partitioned on `msg_date`.
- Added a NumPy-based Python publisher that emits random numeric payloads to configurable MQTT topics and publish intervals.
- Added a local smoke-test path that boots the stack, runs the ingestor, publishes sample data, and verifies inserted rows.
- Added trace-aware local query helpers for comparing publish, receive, and commit timing across events.
- Added a local packet-capture helper for observing MQTT protocol traffic alongside trace logs and database commits.

### Topic: Event Tracing
- Added traced JSON publisher payloads with `event_id`, `trace_id`, `publisher_id`, `sequence`, and `published_at`.
- Added trace-aware ingestion logging and Postgres persistence fields so publish, receive, and commit timing can be correlated per event.

### Topic: Logging
- Added structured JSON runtime logging for Docker console output with canonical service, MQTT, routing, and database events.
- Added an internal event middleware pipeline with context enrichment and payload redaction.
- Added a human-readable `text` log format for local terminal and debugger sessions alongside the Docker-oriented JSON format.

### Topic: Configuration
- Removed the experimental file-based runtime mapping path.
- Restored direct MQTT, database, and `--route TOPIC_FILTER=TABLE` configuration for local development.

### Topic: Testing
- Added tests for route-based config loading and database schema validation.
- Added logging-focused tests covering event serialization, payload redaction, MQTT lifecycle events, and database write outcomes.

### Topic: Developer Tooling
- Added a repo-local VS Code `launch.json` with ready-to-use Python, pytest, and application debug configurations for the `src/` layout.

### Topic: Documentation
- Reduced `README.md` to essential quick-start information and moved detailed material into focused files under `docs/`.
- Added a development runbook covering the local TimescaleDB/Mosquitto stack, publishing, queries, smoke tests, cleanup, and troubleshooting.
- Updated `README.md` to document the route-based runtime, Git tag-based versioning, and the Docker logging model.

## 0.1.1

### Topic: Packaging
- Switched `pyproject.toml` from a hard-coded version to dynamic versioning via `setuptools-scm`.
- Updated package version discovery in `src/mqtt2postgres/__init__.py` to read the installed package metadata.

### Topic: Documentation
- Added `Readme.md` as an additional repo-level documentation entrypoint.
- Added this `changelog.md` file with topic-based change entries.

## 0.1.0

### Topic: Project Restructure
- Replaced the legacy proof-of-concept layout with a minimal `src/mqtt2postgres` package.
- Removed YAML config, SQL bootstrap artifacts, experimental scripts, and demo FastAPI code.

### Topic: CLI Runtime
- Added a CLI entrypoint for MQTT-to-Postgres ingestion with repeated `--map <topic>=<table>` routing.
- Added explicit database and MQTT connection flags for host, port, user, and password.

### Topic: Operations
- Added `environment.yml` for conda-based setup.
- Added a minimal Docker image definition for the ingestor runtime.

### Topic: Testing
- Added focused tests for config parsing, topic matching, and database insert statement construction.

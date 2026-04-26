# Changelog

## 0.9.1 - 2026-04-26

### Topic: Testing
- Added `tests/conftest.py` so plain `pytest` from the repo root resolves the `src/` layout without requiring `PYTHONPATH=src`.
- Updated the MQTT password validation test to match the current subscriber settings error message.

## 0.9.0 - 2026-04-26

### Topic: App Structure
- Restructured `src/` around symmetric `apps.publisher` and `apps.subscriber` packages, each with aligned `__main__`, `cli`, `settings`, `models`, and `runtime` modules.
- Moved aggregate-driven twin-config generation into `apps.publisher.twin_config`.
- Kept `src/mqtt2postgres` as the subscriber package entrypoint and package metadata surface only.

### Topic: Cleanup
- Removed deprecated compatibility modules and old app-specific trees under `src/mqtt2postgres`, `src/settings`, `src/ingest`, and `src/broker/publisher`.
- Removed the redundant repo-root `main.py` wrapper.
- Standardized publisher startup on `python -m apps.publisher` and subscriber startup on `python -m mqtt2postgres`.

### Topic: Shared Runtime
- Kept shared MQTT subscriber helpers under `src/broker/subscriber`.
- Kept shared logging and trace helpers under `src/observability`.

### Topic: Testing
- Repointed tests to the canonical `apps.publisher` and `apps.subscriber` modules.
- Added a structural test to enforce the aligned publisher/subscriber app layout and smoke-import the canonical entrypoints.

### Topic: Documentation
- Updated README, Docker, configuration, runtime, and development docs to reflect the canonical app layout and commands.

## 0.8.0 - 2026-04-26

### Topic: Simulation
- Added aggregate-driven twin-config helpers that generate `publisher-config.json` style output from retained aggregate tables.
- Extended the publisher runtime to support both `uniform` and `clipped_normal` topic generators.

### Topic: Runtime Structure
- Added a dedicated `mqtt2postgres.app` entrypoint and switched the package script and Docker entrypoint to use it.
- Moved broker-facing MQTT helpers into `src/broker`, including a split publisher package with separate models, config loading, runtime, and CLI modules.
- Moved subscriber ingestion runtime code into `src/ingest`.
- Moved trace payload and runtime logging helpers into `src/observability`.

### Topic: Configuration
- Simplified subscriber runtime configuration so `mqtt2postgres` now accepts only `--config` for bootstrap and resolves subscriber settings from JSON config plus environment defaults.
- Removed subscriber support for per-setting runtime CLI overrides such as direct `--mqtt-host`, `--db-host`, `--topic-filter`, and related flags.

### Topic: Compatibility
- Deprecated `mqtt2postgres.cli` in favor of `mqtt2postgres.app` and `python -m mqtt2postgres`.
- Deprecated `mqtt2postgres.publisher` in favor of `broker.publisher`.
- Kept compatibility shim modules for `mqtt2postgres.mqtt`, `mqtt2postgres.service`, `mqtt2postgres.runtime_logging`, `mqtt2postgres.tracing`, and `mqtt2postgres.publisher` during the refactor.

### Topic: Documentation
- Updated runtime, Docker, and configuration docs to reflect JSON-config-first subscriber configuration and the new entrypoint layout.

## 0.7.1 - 2026-04-25

### Topic: Documentation
- Added dedicated ingest-pipeline documentation with a Mermaid flowchart showing the path from subscribers through database ingest functions into raw, topic-overview, and aggregate tables.

## 0.7.0 - 2026-04-25

### Topic: Aggregation
- Added explainable aggregate data-quality scoring with `quality_score`, quality sub-scores, and `quality_flags` distinct from the technical bucket `status`.
- Added percentile summaries to every aggregate table: median, 25th percentile, and 75th percentile of the raw numeric values in each bucket.
- Added statistical trust metrics to every aggregate table: sample variance, sample standard deviation, standard error, and 95% confidence bounds for the bucket mean.
- Added interval-regularity metrics and `quality_interval_score` so retained bucket quality also reflects how evenly measurements are distributed over time.
- Updated the shared aggregate refresh helper to compute trust metrics from the raw numeric values inside each bucket.
- Added short SQL comments for the aggregate tables and their columns so the retained summary fields remain self-describing in the database.
- Added a 24-hour aggregate query helper and updated the smoke test to verify trust-metric columns, populated quality scoring, and completed rated aggregate rows.
- Added dedicated documentation for aggregate `status` and `quality`, including a Mermaid statechart and the implemented score conditions.

## 0.6.0 - 2026-04-25

### Topic: Aggregation
- Added stored 15-minute and 60-minute aggregate hypertables with the same device-level boundary-aware metrics as the existing 3-minute aggregates.
- Added stored 24-hour aggregate hypertables with the same device-level boundary-aware metrics as the shorter aggregate windows.
- Added `mqtt_ingest.refresh_message_15m_aggregates(...)` and `mqtt_ingest.refresh_message_60m_aggregates(...)`, plus matching TimescaleDB background jobs.
- Added `mqtt_ingest.refresh_message_24h_aggregates(...)`, plus a matching TimescaleDB background job.
- Changed `mqtt_ingest.ingest_message(...)` to refresh the touched 3-minute, 15-minute, 60-minute, and 24-hour buckets on each message insert.
- Added helper scripts for inspecting local 15-minute and 60-minute aggregate rows.
- Refactored the aggregate SQL bootstrap around shared helper functions so new bucket widths no longer require duplicating the full table, refresh, and job logic.

### Topic: Topic Overview
- Added `mqtt_ingest.topic_overview` and `mqtt_ingest.ingest_topics(...)` for tracking distinct published topics, last activity, and recent trace metadata.
- Added a second local subscriber service configured to subscribe broadly and populate the topic overview table.
- Updated the topic-overview subscriber config to include broker `$SYS/#` status topics alongside normal application topics.

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

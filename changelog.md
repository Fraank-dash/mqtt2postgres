# Changelog

## Unreleased

### Topic: Local Testing
- Added a local Mosquitto + Postgres compose stack and wrapper scripts for quick MQTT development testing.
- Added a NumPy-based Python publisher that emits random numeric payloads to configurable MQTT topics and publish intervals.
- Added a local smoke-test path that boots the stack, runs the ingestor, publishes sample data, and verifies inserted rows.

### Topic: Logging
- Added structured JSON runtime logging for Docker console output with canonical service, MQTT, routing, database, and config events.
- Added an internal event middleware pipeline with context enrichment and payload redaction.
- Added startup snapshot diff logging for broker and derived contract add, remove, and change detection.
- Added a human-readable `text` log format for local terminal and debugger sessions alongside the Docker-oriented JSON format.

### Topic: Data Contracts
- Switched runtime table mapping from raw table names to ODCS contract files.
- Added ODCS sample contracts under `contracts/` and contract loading/validation in the application runtime.
- Aligned Postgres credential handling with Datacontract-style environment variables.
- Refactored the runtime to a two-layer contract model with one raw broker contract and one or more derived Postgres contracts.

### Topic: Testing
- Added contract-focused tests for ODCS parsing, contract-backed config loading, and database schema validation against a contract.
- Added logging-focused tests covering event serialization, payload redaction, snapshot diffing, MQTT lifecycle events, and database write outcomes.

### Topic: Developer Tooling
- Added a repo-local VS Code `launch.json` with ready-to-use Python, pytest, and application debug configurations for the `src/` layout.

### Topic: Documentation
- Updated `README.md` to document the ODCS-driven runtime, Datacontract CLI lint/test workflow, Git tag-based versioning, the Docker logging model, and snapshot-backed config diffing.

### Topic: Container
- Updated the Docker image to include the example `contracts/` directory used by the documented runtime commands.

### Topic: Versioning
- Bumped the `setuptools-scm` fallback version to `0.2.0` for the contract-based runtime changes.

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

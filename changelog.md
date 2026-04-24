# Changelog

## Unreleased

### Topic: Data Contracts
- Switched runtime table mapping from raw table names to ODCS contract files.
- Added ODCS sample contracts under `contracts/` and contract loading/validation in the application runtime.
- Aligned Postgres credential handling with Datacontract-style environment variables.
- Refactored the runtime to a two-layer contract model with one raw broker contract and one or more derived Postgres contracts.

### Topic: Testing
- Added contract-focused tests for ODCS parsing, contract-backed config loading, and database schema validation against a contract.

### Topic: Documentation
- Updated `README.md` to document the ODCS-driven runtime, Datacontract CLI lint/test workflow, Git tag-based versioning, and the long-running Docker container behavior.

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

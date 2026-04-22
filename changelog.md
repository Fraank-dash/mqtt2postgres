# Changelog

## Unreleased

### Topic: Documentation
- Clarified in `README.md` that the Docker container is expected to remain running because the ingestor is a long-lived consumer process.

### Topic: Versioning
- Bumped the fallback package version to `0.1.1`.

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

# mqtt2postgres

`mqtt2postgres` subscribes to MQTT topic filters and writes matching messages into PostgreSQL-compatible tables.

The local development stack uses Mosquitto and TimescaleDB 16.

## Quick Start

Start the full local development stack:

```bash
docker compose -f examples/local-stack/docker-compose.yml up --build
```

This starts four containers:

- `mqtt-publisher`: publishes random traced MQTT payloads continuously
- `mqtt-broker`: local Mosquitto broker
- `mqtt-subscriber`: subscribes to MQTT topics and writes matching messages
- `timescaledb`: local TimescaleDB 16 database

Check stored rows:

```bash
./scripts/dev/query-local-sensor-temp.sh
```

## Further Information

- [Development runbook](docs/development-runbook.md): local stack, smoke tests, publishing, queries, cleanup, troubleshooting.
- [Configuration](docs/configuration.md): CLI flags, environment variables, and route mapping.
- [Runtime behavior](docs/runtime-behavior.md): routing, table requirements, and logging events.
- [Docker usage](docs/docker.md): build/run commands and container networking notes.
- [Versioning](docs/versioning.md): Git tag based package versions.

## Repository Layout

- `src/mqtt2postgres/`: application package
- `examples/`: local stack, SQL bootstrap files, publisher wrapper
- `scripts/dev/`: development helper scripts
- `docs/`: detailed documentation

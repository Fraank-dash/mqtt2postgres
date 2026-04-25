# mqtt2postgres

`mqtt2postgres` subscribes to MQTT topic filters and passes messages to a PostgreSQL/TimescaleDB ingest function.

The local development stack uses Mosquitto and TimescaleDB 16 with Timescale Toolkit-enabled local SQL bootstrap.

## Quick Start

Start the full local development stack:

```bash
docker compose -f examples/local-stack/docker-compose.yml up --build
```

This starts four containers:

- `mqtt-publisher`: publishes random traced MQTT payloads continuously
- `mqtt-broker`: local Mosquitto broker
- `mqtt-subscriber`: subscribes to MQTT topics and calls the database ingest function
- `timescaledb`: local TimescaleDB 16 database

Check stored rows:

```bash
./scripts/dev/query-local-sensor-temp.sh
```

Check 3-minute aggregates:

```bash
./scripts/dev/query-local-3m-aggregates.sh
```

The local bootstrap stores raw messages in `mqtt_ingest.messages` and maintains 3-minute topic aggregates in `mqtt_ingest.message_3m_aggregates`, including plain in-bucket stats plus LOCF and linear-interpolated boundary values.

## Further Information

- [Development runbook](docs/development-runbook.md): local stack, smoke tests, publishing, queries, cleanup, troubleshooting.
- [Configuration](docs/configuration.md): CLI flags, environment variables, topic filters, and ingest function settings.
- [Runtime behavior](docs/runtime-behavior.md): MQTT subscription, database-function ingest, 3-minute aggregates, and logging events.
- [Docker usage](docs/docker.md): build/run commands and container networking notes.
- [Versioning](docs/versioning.md): Git tag based package versions.

## Repository Layout

- `src/mqtt2postgres/`: application package
- `examples/`: local stack, SQL bootstrap files, publisher wrapper
- `scripts/dev/`: development helper scripts
- `docs/`: detailed documentation

# mqtt2postgres

`mqtt2postgres` subscribes to MQTT topic filters and passes messages to a PostgreSQL/TimescaleDB ingest function.

The local development stack uses Mosquitto and TimescaleDB 16 with Timescale Toolkit-enabled local SQL bootstrap.

## Quick Start

Start the full local development stack:

```bash
docker compose -f examples/local-stack/docker-compose.yml up --build
```

This starts five containers:

- `mqtt-publisher`: publishes random traced MQTT payloads continuously from a mounted JSON config
- `mqtt-broker`: local Mosquitto broker
- `mqtt-subscriber`: subscribes to sensor topics from a mounted JSON config and calls the aggregate/raw ingest function
- `mqtt-subscriber-topics`: subscribes to all broker topics from a mounted JSON config and records topic overview data
- `timescaledb`: local TimescaleDB 16 database

Check stored rows:

```bash
./scripts/dev/query-local-sensor-temp.sh
```

Check 3-minute aggregates:

```bash
./scripts/dev/query-local-3m-aggregates.sh
```

Check 15-minute aggregates:

```bash
./scripts/dev/query-local-15m-aggregates.sh
```

Check 60-minute aggregates:

```bash
./scripts/dev/query-local-60m-aggregates.sh
```

The local bootstrap stores raw messages in `mqtt_ingest.messages` and maintains 3-minute, 15-minute, and 60-minute aggregates in `mqtt_ingest.message_3m_aggregates`, `mqtt_ingest.message_15m_aggregates`, and `mqtt_ingest.message_60m_aggregates`, including parsed `device_id` and `metric_name` dimensions for topics shaped like `sensors/<device>/<metric>`, plain in-bucket stats, and LOCF and linear-interpolated boundary values.

The local publisher service reads `/config/publisher-config.json` from a read-only Docker volume so topic and generator setup no longer has to live in the Compose command.
The local subscriber service does the same with `/config/subscriber-config.json` for broker, database, and topic-filter settings. A second subscriber uses `/config/subscriber-topics-config.json` to feed `mqtt_ingest.ingest_topics` and keep a topic inventory, including broker `$SYS/#` status topics.

## Further Information

- [Development runbook](docs/development-runbook.md): local stack, smoke tests, publishing, queries, cleanup, troubleshooting.
- [Configuration](docs/configuration.md): CLI flags, environment variables, topic filters, and ingest function settings.
- [Runtime behavior](docs/runtime-behavior.md): MQTT subscription, database-function ingest, stored aggregates, and logging events.
- [Docker usage](docs/docker.md): build/run commands and container networking notes.
- [Versioning](docs/versioning.md): Git tag based package versions.

## Repository Layout

- `src/mqtt2postgres/`: application package
- `examples/`: local stack, SQL bootstrap files, publisher wrapper
- `scripts/dev/`: development helper scripts
- `docs/`: detailed documentation

# mqtt2postgres

`mqtt2postgres` is now primarily a local MQTT-to-TimescaleDB bench for simulating device data, ingesting it through database functions, and retaining both raw events and derived aggregates.

The current focus is [`examples/local-stack`](examples/local-stack/docker-compose.yml), which gives you a Docker-based environment for:

- simulating multiple devices and metrics from JSON config
- subscribing to selected MQTT topic patterns
- ingesting messages through PostgreSQL/TimescaleDB functions
- keeping a broker-wide topic inventory
- building retained `3m`, `15m`, `60m`, and `24h` aggregates with quality scoring
- reconciling `power` integration against cumulative `energy` counters per device and bucket

If you want a short mental model, this repo is closer to a small IoT simulation and ingestion bench than to a one-off MQTT subscriber script.

## Primary Use Case

The main workflow is the Compose stack in `examples/local-stack`.

It starts:

- `mqtt-publisher`: JSON-configured synthetic publishers for multiple devices and topics
- `mqtt-broker`: local Mosquitto broker
- `mqtt-subscriber`: sensor ingestor that calls `mqtt_ingest.ingest_message`
- `mqtt-subscriber-topics`: broker-overview ingestor that calls `mqtt_ingest.ingest_topics`
- `timescaledb`: local TimescaleDB 16 with `timescaledb_toolkit`

This is useful for:

- testing topic structures such as `sensors/<device>/<metric>`
- simulating small fleets of devices with per-topic generators
- exploring ingestion-function based database designs
- validating aggregate behavior after raw data is eventually pruned
- inspecting topic coverage, boundary interpolation, trust metrics, and quality scoring

## Quick Start

Start the full local stack:

```bash
docker compose -f examples/local-stack/docker-compose.yml up --build
```

Or use the helper script:

```bash
./scripts/dev/reset-local-test-stack.sh
```

The important host endpoints are:

- MQTT broker: `127.0.0.1:1883`
- MQTT broker with Technitium DNS: `mqtt.pi5.local:1883`
- Postgres/TimescaleDB: `127.0.0.1:55432`
- database: `mqtt`
- username/password: `postgres` / `postgres`

The broker now requires MQTT authentication. The example local stack ships with separate publisher and subscriber accounts from `examples/local-stack/mosquitto/`, which is now a public broker submodule checkout. Before first startup, populate `examples/local-stack/mosquitto/passwords` from `passwords.example` or switch that submodule to your private broker branch.

Stop and fully reset the stack:

```bash
docker compose -f examples/local-stack/docker-compose.yml down -v
```

Use the reset path after SQL bootstrap changes so the database is recreated from `examples/sql/mqtt-ingest`.
That path is now a Git submodule checkout.

## Simulation Model

The local publisher is configured from [`examples/local-stack/publisher-config.json`](examples/local-stack/publisher-config.json).
Publisher settings files can be JSON or YAML as long as they follow the same document structure.

One settings file can define:

- multiple publishers
- multiple devices
- multiple topics per publisher
- one independent generator per topic

Supported topic generator kinds:

- `uniform`
- `clipped_normal`

The default example simulates two devices:

- `node-1`
- `node-2`

And two metrics per device:

- `temp`
- `humidity`

This is why “simple digital twin” is a reasonable description of the current local stack, as long as it is understood as lightweight sensor and topic simulation rather than a full digital-twin platform.

## Aggregate-Driven Twin Generation

Retained aggregate tables can now be turned back into simulated publisher configs.

Example:

```bash
PYTHONPATH=src mqtt2postgres-twin-config \
  --db-host 127.0.0.1 \
  --db-port 55432 \
  --db-name mqtt \
  --db-user postgres \
  --db-password postgres \
  --topic-filter 'sensors/+/temp'
```

The helper:

- reads aggregate tables, defaulting to `mqtt_ingest.message_24h_aggregates`
- filters for usable retained buckets
- infers generator parameters and publish cadence
- emits a full `publisher-config.json` style document

The first version prefers:

- `clipped_normal` when mean and spread are usable
- `uniform` when only safe bounds can be learned

This makes it possible to learn a lightweight simulated twin from retained aggregate behavior and feed it back into the local Docker bench.

## Ingestion Model

The stack has two subscriber paths:

1. Sensor ingest
   - config: [`examples/local-stack/subscriber-config.json`](examples/local-stack/subscriber-config.json)
   - topic filters: focused patterns such as `sensors/+/temp` and `sensors/+/humidity`
   - database function: `mqtt_ingest.ingest_message`
   - outputs:
     - `mqtt_ingest.messages`
     - `mqtt_ingest.message_3m_aggregates`
     - `mqtt_ingest.message_15m_aggregates`
     - `mqtt_ingest.message_60m_aggregates`
     - `mqtt_ingest.message_24h_aggregates`
     - `mqtt_ingest.power_energy_3m_reconciliation`
     - `mqtt_ingest.power_energy_15m_reconciliation`
     - `mqtt_ingest.power_energy_60m_reconciliation`
     - `mqtt_ingest.power_energy_24h_reconciliation`

2. Topic-overview ingest
   - config: [`examples/local-stack/subscriber-topics-config.json`](examples/local-stack/subscriber-topics-config.json)
   - topic filters: `#` and `$SYS/#`
   - database function: `mqtt_ingest.ingest_topics`
   - output:
     - `mqtt_ingest.topic_overview`

For parsed sensor topics shaped like `sensors/<device>/<metric>`, the aggregate rows are grouped by:

- `bucket_start`
- `device_id`
- `metric_name`

## What Gets Stored

Raw table:

- `mqtt_ingest.messages`
- stores the original MQTT topic and payload
- extracts numeric values when possible
- stores trace metadata
- stores parsed `device_id` and `metric_name` when the topic matches `sensors/<device>/<metric>`

Topic inventory table:

- `mqtt_ingest.topic_overview`
- stores one row per seen topic
- includes broker status topics via `$SYS/#`

Aggregate tables:

- `mqtt_ingest.message_3m_aggregates`
- `mqtt_ingest.message_15m_aggregates`
- `mqtt_ingest.message_60m_aggregates`
- `mqtt_ingest.message_24h_aggregates`

These aggregate tables now include:

- plain in-bucket stats
- median, p25, p75
- variance, stddev, stderr, 95% CI
- LOCF and linear boundary values
- time-weighted averages
- interval-regularity metrics
- technical `status`
- analytical `quality_score` and explainability fields

Power/energy reconciliation tables:

- `mqtt_ingest.power_energy_3m_reconciliation`
- `mqtt_ingest.power_energy_15m_reconciliation`
- `mqtt_ingest.power_energy_60m_reconciliation`
- `mqtt_ingest.power_energy_24h_reconciliation`

These tables compare two derived energy estimates for `sensors/<device>/power` and `sensors/<device>/energy`:

- cumulative `energy` deltas across the bucket
- `power` integrated across the same bucket using both LOCF and linear methods

## Typical Workflow

For most work, use Docker Compose first and the helper scripts second.

1. Edit the JSON configs in `examples/local-stack/`.
2. Start the stack with `docker compose`.
3. Watch container logs.
4. Inspect tables directly with `psql` or the helper query scripts.
5. Optionally generate a learned publisher config from aggregates.
6. Adjust publisher topics/generators and subscriber filters as needed.

The `.sh` helpers are still useful, but they are now mainly convenience tools for inspection and smoke testing rather than the main product surface.

## Useful Inspection Commands

All services:

```bash
docker compose -f examples/local-stack/docker-compose.yml logs -f
```

Sensor ingestor only:

```bash
docker compose -f examples/local-stack/docker-compose.yml logs -f mqtt-subscriber
```

Topic overview ingestor only:

```bash
docker compose -f examples/local-stack/docker-compose.yml logs -f mqtt-subscriber-topics
```

Publisher only:

```bash
docker compose -f examples/local-stack/docker-compose.yml logs -f mqtt-publisher
```

Direct SQL shell:

```bash
docker compose -f examples/local-stack/docker-compose.yml exec -T timescaledb \
  psql -U postgres -d mqtt
```

Optional helper scripts:

```bash
./scripts/dev/query-local-sensor-temp.sh
./scripts/dev/query-local-topic-overview.sh
./scripts/dev/query-local-3m-aggregates.sh
./scripts/dev/query-local-15m-aggregates.sh
./scripts/dev/query-local-60m-aggregates.sh
./scripts/dev/query-local-24h-aggregates.sh
./scripts/dev/query-local-power-energy-reconciliation.sh
```

Generate a learned publisher config from retained aggregates:

```bash
PYTHONPATH=src mqtt2postgres-twin-config \
  --db-host 127.0.0.1 \
  --db-port 55432 \
  --db-name mqtt \
  --db-user postgres \
  --db-password postgres \
  --topic-filter 'sensors/+/temp' \
  --output generated-publisher-config.json
```

## Further Information

- [Development runbook](docs/development-runbook.md): local stack, smoke tests, publishing, queries, cleanup, troubleshooting
- [Use cases](docs/use-cases.md): common simulation, ingest, topic-inventory, twin-generation, debugging, and power-energy reconciliation scenarios
- [Configuration](docs/configuration.md): subscriber settings files, publisher settings files, environment variables, topic filters, and ingest function settings
- [System architecture](docs/system-architecture.md): component responsibilities, cardinalities, and Mermaid class and sequence diagrams
- [Runtime behavior](docs/runtime-behavior.md): MQTT subscription, database-function ingest, stored aggregates, and logging events
- [Ingest pipeline](docs/ingest-pipeline.md): flowchart from subscribers through database functions into raw, topic-overview, and aggregate tables
- [Aggregate status and quality](docs/aggregate-status-and-quality.md): bucket lifecycle, quality scoring inputs, and Mermaid statechart
- [Docker usage](docs/docker.md): build/run commands and container networking notes
- [Versioning](docs/versioning.md): Git tag based package versions

## Repository Layout

- `examples/local-stack/`: primary local bench and JSON configs
- `examples/sql/mqtt-ingest`: standalone SQL submodule checkout for TimescaleDB bootstrap and ingest SQL
- `src/apps/`: canonical subscriber and publisher app packages
- `src/broker/`: shared broker protocol/client helpers
- `src/observability/`: shared logging and tracing helpers
- `src/mqtt2postgres/`: subscriber package entrypoint and package metadata
- `scripts/dev/`: optional helper scripts
- `docs/`: detailed documentation

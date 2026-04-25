# Development Runbook

This runbook covers the local development workflow with Mosquitto, TimescaleDB 16, the MQTT subscriber/ingestor, and the synthetic publisher.

## Requirements

The default workflow uses Docker Compose. The optional smoke-test script also uses the local Python environment from `environment.yml`.

## Start The Four-Container Stack

Start the full local development stack:

```bash
docker compose -f examples/local-stack/docker-compose.yml up --build
```

This starts:

- `mqtt-publisher`: publishes random traced payloads continuously from `examples/local-stack/publisher-config.json`.
- `mqtt-broker`: local Mosquitto broker.
- `mqtt-subscriber`: subscribes to the broker from `examples/local-stack/subscriber-config.json` and calls `mqtt_ingest.ingest_message`.
- `mqtt-subscriber-topics`: subscribes to all broker topics from `examples/local-stack/subscriber-topics-config.json` and calls `mqtt_ingest.ingest_topics`.
- `timescaledb`: local TimescaleDB 16 database.

The local database service uses the Timescale HA Docker image so `timescaledb_toolkit` is available during bootstrap.

The stack exposes these host ports for manual inspection:

- MQTT broker: `127.0.0.1:1883`
- TimescaleDB/Postgres protocol: `127.0.0.1:55432`
- Database: `mqtt`
- User/password: `postgres` / `postgres`

The Compose service for the database is named `timescaledb`.

TimescaleDB starts with one generic ingest target: `mqtt_ingest.messages`. You do not need to create per-topic tables before subscribing.

## View Logs

All services:

```bash
docker compose -f examples/local-stack/docker-compose.yml logs -f
```

Subscriber/ingestor only:

```bash
docker compose -f examples/local-stack/docker-compose.yml logs -f mqtt-subscriber
```

Publisher only:

```bash
docker compose -f examples/local-stack/docker-compose.yml logs -f mqtt-publisher
```

Broker only:

```bash
docker compose -f examples/local-stack/docker-compose.yml logs -f mqtt-broker
```

## Query Stored Data

Inspect recent sensor rows, including parsed `device_id` and `metric_name`:

```bash
./scripts/dev/query-local-sensor-temp.sh
```

Compare publish, receive, and commit timing for the latest traced events:

```bash
./scripts/dev/query-local-trace-report.sh
```

Inspect 3-minute aggregate rows:

```bash
./scripts/dev/query-local-3m-aggregates.sh
```

Inspect 15-minute aggregate rows:

```bash
./scripts/dev/query-local-15m-aggregates.sh
```

Inspect 60-minute aggregate rows:

```bash
./scripts/dev/query-local-60m-aggregates.sh
```

The aggregate queries include `device_id`, `metric_name`, plain in-bucket stats, and the LOCF and linear boundary columns with their corresponding time-weighted averages.

Inspect the topic overview table:

```bash
./scripts/dev/query-local-topic-overview.sh
```

Direct database shell:

```bash
docker compose -f examples/local-stack/docker-compose.yml exec -T timescaledb \
  psql -U postgres -d mqtt
```

## Stop And Reset

Stop the stack and remove volumes:

```bash
./scripts/dev/stop-local-test-stack.sh
```

Use this after SQL bootstrap changes so TimescaleDB reinitializes the tables and hypertables.
The SQL bootstrap now also enables `timescaledb_toolkit`, so the Timescale image must include that extension.

## Override Publisher Settings

The default `mqtt-publisher` service mounts `examples/local-stack/publisher-config.json` into the container at `/config/publisher-config.json`.
The default `mqtt-subscriber` service mounts `examples/local-stack/subscriber-config.json` into the container at `/config/subscriber-config.json`.
The topic-overview subscriber mounts `examples/local-stack/subscriber-topics-config.json` into the container at `/config/subscriber-topics-config.json`.

Edit that JSON file to change the publisher set, topics, or generator ranges without rewriting the Compose command.
Edit the subscriber JSON file to change broker/database settings or the topic filter list without rewriting the Compose command.
Edit the topic-overview subscriber JSON file to change the overview ingest function or the topic filter scope without rewriting the Compose command. The default config includes both `#` and `$SYS/#` so broker status topics are recorded in `mqtt_ingest.topic_overview`.

For a temporary one-off publisher command, run:

```bash
docker compose -f examples/local-stack/docker-compose.yml run --rm mqtt-publisher \
  'exec python -m mqtt2postgres.publisher --topic sensors/node-2/temp --min-value 20 --max-value 30 --frequency-seconds 0.5'
```

The JSON format supports multiple publisher entries, and each publisher can define multiple topics with independent generators. Each topic generates its own value stream on every publish cycle.

## Manual Host Workflow

The four-container stack is the default development workflow. You can still run the ingestor and publisher directly on the host for debugging.

Create and activate the conda environment:

```bash
conda env create -f environment.yml
conda activate mqtt2postgres
```

Start only the local infrastructure:

```bash
docker compose -f examples/local-stack/docker-compose.yml up mqtt-broker timescaledb
```

Then run the ingestor locally in another terminal:


```bash
export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=postgres

python main.py \
  --config path/to/subscriber.json
```

Or configure the subscriber directly with flags:

```bash
python main.py \
  --log-format text \
  --log-level DEBUG \
  --mqtt-host 127.0.0.1 \
  --mqtt-port 1883 \
  --db-host 127.0.0.1 \
  --db-port 55432 \
  --db-name mqtt \
  --topic-filter 'sensors/+/temp' \
  --db-ingest-function mqtt_ingest.ingest_message
```

The ingestor is a long-running subscriber. Leave it running while publishing messages.

Publish five traced random values from the host:


```bash
python examples/publish_random.local.py \
  --host 127.0.0.1 \
  --port 1883 \
  --topic sensors/node-1/temp \
  --min-value 0 \
  --max-value 10 \
  --frequency-seconds 1 \
  --count 5 \
  --seed 7
```

Useful publisher options:

- `--count 5` publishes a fixed number of messages and exits.
- `--qos 1` changes MQTT publish QoS.
- `--seed 7` makes generated values repeatable.
- `--trace-id demo-trace-1` forces one shared trace id for the run.
- `--payload-format plain` publishes raw numeric payloads instead of traced JSON.
- `--config path/to/publisher.json` loads one JSON file containing multiple publishers and multiple topics per publisher.
- `--config path/to/subscriber.json` loads one JSON file for one subscriber with multiple topic filters.

Without `--count`, the publisher runs until stopped.

## Smoke Test

Run the full smoke path:

```bash
./scripts/dev/run-local-smoke-test.sh
```

The smoke script is deterministic and separate from the continuous four-container workflow. It starts only `mqtt-broker` and `timescaledb`, runs the ingestor on the host, publishes traced messages for multiple devices under `sensors/+/temp`, and verifies rows in `mqtt_ingest.messages`, `mqtt_ingest.message_3m_aggregates`, `mqtt_ingest.message_15m_aggregates`, and `mqtt_ingest.message_60m_aggregates`.

If your Python executable is not `python3`, override it:

```bash
PYTHON_BIN=python ./scripts/dev/run-local-smoke-test.sh
```

## Capture MQTT Traffic

Capture MQTT packets during a local run:

```bash
./scripts/dev/capture-mqtt-traffic.sh
```

The capture is written under `.tmp/` as a `.pcap` file for tools such as Wireshark or `tshark`.

## Troubleshooting

### `postgres is not running`

The local database Compose service is named `timescaledb`. Scripts should use:

```bash
docker compose -f examples/local-stack/docker-compose.yml exec -T timescaledb ...
```

### `localhost:5432 connection refused` from Docker

Inside an ingestor container, `localhost` points to the ingestor container. The four-container Compose stack uses service DNS names: `mqtt-broker` and `timescaledb`.

For standalone `docker run` against host-published ports, use `host.docker.internal`.

### `ModuleNotFoundError: No module named 'numpy'`

Activate the conda environment before running the publisher or smoke test:

```bash
conda activate mqtt2postgres
```

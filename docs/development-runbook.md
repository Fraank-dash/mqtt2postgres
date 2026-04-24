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

- `mqtt-publisher`: publishes random traced payloads continuously to `sensors/node-1/temp`.
- `mqtt-broker`: local Mosquitto broker.
- `mqtt-subscriber`: subscribes to the broker and writes matching messages into TimescaleDB.
- `timescaledb`: local TimescaleDB 16 database.

The stack exposes these host ports for manual inspection:

- MQTT broker: `127.0.0.1:1883`
- TimescaleDB/Postgres protocol: `127.0.0.1:55432`
- Database: `mqtt`
- User/password: `postgres` / `postgres`

The Compose service for the database is named `timescaledb`.

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

Inspect recent sensor rows:

```bash
./scripts/dev/query-local-sensor-temp.sh
```

Compare publish, receive, and commit timing for the latest traced events:

```bash
./scripts/dev/query-local-trace-report.sh
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

## Override Publisher Settings

The default `mqtt-publisher` command is defined in `examples/local-stack/docker-compose.yml`.

For a temporary one-off publisher command, run:

```bash
docker compose -f examples/local-stack/docker-compose.yml run --rm mqtt-publisher \
  'exec python -m mqtt2postgres.publisher --host mqtt-broker --port 1883 --topic sensors/node-2/temp --min-value 20 --max-value 30 --frequency-seconds 0.5'
```

For persistent changes, edit the `mqtt-publisher` service command in the Compose file.

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
  --log-format text \
  --log-level DEBUG \
  --mqtt-host 127.0.0.1 \
  --mqtt-port 1883 \
  --db-host 127.0.0.1 \
  --db-port 55432 \
  --db-name mqtt \
  --db-schema public \
  --route 'sensors/+/temp=tbl_sensor_temp' \
  --route '$SYS/broker/messages/#=tbl_broker_metrics'
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

Without `--count`, the publisher runs until stopped.

## Smoke Test

Run the full smoke path:

```bash
./scripts/dev/run-local-smoke-test.sh
```

The smoke script is deterministic and separate from the continuous four-container workflow. It starts only `mqtt-broker` and `timescaledb`, runs the ingestor on the host, publishes five traced messages, and verifies rows in `public.tbl_sensor_temp`.

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

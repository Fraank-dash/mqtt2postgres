# mqtt2postgres

Minimal CLI tool that reads a raw MQTT broker contract, then writes trusted derived values into Postgres tables defined by ODCS data contracts.

## Local environment

Create and activate the conda environment:

```bash
conda env create -f environment.yml
conda activate mqtt2postgres
```

Run the tool through the root wrapper:

```bash
export DATACONTRACT_POSTGRES_USERNAME=postgres
export DATACONTRACT_POSTGRES_PASSWORD=postgres

python main.py \
  --mqtt-user admin \
  --mqtt-password secret \
  --broker-contract contracts/raw/mqtt_broker.odcs.yaml \
  --derived-contract contracts/derived/tbl_broker_metrics.odcs.yaml \
  --derived-contract contracts/derived/tbl_sensor_temp.odcs.yaml
```

The runtime now uses a two-layer pattern:

- one raw broker contract that defines MQTT connectivity and topic ownership
- one or more derived contracts that define trusted Postgres outputs and the `sourceTopicFilters` they consume

The app loads the broker contract for subscriptions, loads the derived contracts for table validation and routing, and writes only to the contracted tables.

If you prefer compatibility with older local setup, `POSTGRES_USERNAME` and `POSTGRES_PASSWORD` are still accepted as a fallback, but the primary env vars are:

```bash
export DATACONTRACT_POSTGRES_USERNAME=postgres
export DATACONTRACT_POSTGRES_PASSWORD=postgres
export MQTT_USERNAME=admin
export MQTT_PASSWORD=secret
```

## Contracts

The repo contains example ODCS files in [contracts](</home/fraank/coding/mqtt2postgres/contracts>):

- [raw/mqtt_broker.odcs.yaml](/home/fraank/coding/mqtt2postgres/contracts/raw/mqtt_broker.odcs.yaml:1)
- [derived/tbl_sensor_temp.odcs.yaml](/home/fraank/coding/mqtt2postgres/contracts/derived/tbl_sensor_temp.odcs.yaml:1)
- [derived/tbl_broker_metrics.odcs.yaml](/home/fraank/coding/mqtt2postgres/contracts/derived/tbl_broker_metrics.odcs.yaml:1)

The broker contract currently must define:

- one `custom` server
- MQTT transport details in `servers[].customProperties`
- `topicFilters` describing the raw topics owned by the producer

Each derived contract currently must define:

- one `postgres` server
- exactly one table model
- the required fields `msg_date`, `msg_topic`, and `msg_value`
- `customProperties.sourceTopicFilters` describing which broker topics feed that output

## Datacontract CLI

The runtime does not depend on `datacontract-cli`, but the workflow is designed to work with it.

Example lint/test commands:

```bash
datacontract lint contracts/raw/mqtt_broker.odcs.yaml
datacontract lint contracts/derived/tbl_sensor_temp.odcs.yaml
datacontract test contracts/derived/tbl_sensor_temp.odcs.yaml
```

`datacontract test` expects the Postgres credentials in the Datacontract env vars shown above.

## Versioning

Package versions are derived from Git tags via `setuptools-scm`.

- Create release tags in the form `v0.2.0`, `v0.2.1`, and so on.
- In GitHub Actions, make sure the checkout step fetches tags, for example with `fetch-depth: 0`.
- The `fallback_version` in `pyproject.toml` is only used when Git tag metadata is unavailable.

## Logging

The runtime writes logs to stdout/stderr in both Docker and local terminal runs.

- `--log-format json` is the default and is the right choice for Docker.
- `--log-format text` is intended for local development and debugger sessions.
- `--log-level INFO` is the default verbosity.
- `--config-snapshot-path` controls where the app stores the last loaded broker/derived contract snapshot for startup diff logging.

The app emits lifecycle, MQTT, routing, database, and config snapshot events such as:

- `service.starting`
- `mqtt.connected`
- `mqtt.subscribed`
- `message.routed`
- `db.write_succeeded`
- `broker.changed`
- `derived_contract.removed`

MQTT payload bodies are not logged. The logs include metadata such as topic, payload size, target table, status, and error details when a write fails.

For local development, a typical command is:

```bash
python main.py \
  --log-format text \
  --log-level DEBUG \
  --broker-contract contracts/raw/mqtt_broker.odcs.yaml \
  --derived-contract contracts/derived/tbl_broker_metrics.odcs.yaml \
  --derived-contract contracts/derived/tbl_sensor_temp.odcs.yaml
```

## Local MQTT Testing

For quick end-to-end testing, run a local Mosquitto + Postgres stack and a synthetic publisher.

Start the local stack in the foreground:

```bash
./scripts/dev/start-local-test-stack.sh
```

Stop the full stack and remove volumes:

```bash
./scripts/dev/stop-local-test-stack.sh
```

Then run the ingestor locally in another terminal:

```bash
export DATACONTRACT_POSTGRES_USERNAME=postgres
export DATACONTRACT_POSTGRES_PASSWORD=postgres

python main.py \
  --log-format text \
  --log-level DEBUG \
  --config-snapshot-path .tmp/local-config-snapshot.json \
  --broker-contract examples/contracts.local/raw/mqtt_broker.odcs.yaml \
  --derived-contract examples/contracts.local/derived/tbl_broker_metrics.odcs.yaml \
  --derived-contract examples/contracts.local/derived/tbl_sensor_temp.odcs.yaml
```

Publish random sensor values that match the existing `sensors/+/temp` contract route:

```bash
python examples/publish_random.local.py \
  --host 127.0.0.1 \
  --port 1883 \
  --topic sensors/node-1/temp \
  --min-value 0 \
  --max-value 10 \
  --frequency-seconds 1 \
  --seed 7
```

Useful publisher options:

- `--count 5` publishes a fixed number of messages and exits
- `--qos 1` changes MQTT publish QoS
- `--seed 7` makes the generated numeric sequence repeatable

The publisher sends raw numeric UTF-8 payloads such as `4.271934`, not JSON.

Inspect the latest ingested rows:

```bash
./scripts/dev/query-local-sensor-temp.sh
```

Run the full smoke path:

```bash
./scripts/dev/run-local-smoke-test.sh
```

The smoke script starts the local compose stack, launches the ingestor against the local contracts, publishes five sensor messages, and verifies that rows appear in `public.tbl_sensor_temp`.

## Runtime behavior

- Target tables must already exist.
- Each derived table must satisfy the fields declared in its contract.
- MQTT subscriptions come from the broker contract.
- Routing comes from each derived contract's `sourceTopicFilters`.
- If multiple derived contracts match the same topic, the first derived contract passed on the CLI wins.
- Broker and derived contract add/remove/change events are detected at startup by diffing the current config against the persisted snapshot file.

## Docker

Build the image:

```bash
docker build -t mqtt2postgres .
```

Run it with the same CLI flags and environment variables you use locally:

```bash
docker run --rm \
  -e DATACONTRACT_POSTGRES_USERNAME=postgres \
  -e DATACONTRACT_POSTGRES_PASSWORD=postgres \
  -v "$(pwd)/state:/var/lib/mqtt2postgres" \
  mqtt2postgres \
  --log-level INFO \
  --config-snapshot-path /var/lib/mqtt2postgres/config-snapshot.json \
  --mqtt-user admin \
  --mqtt-password secret \
  --broker-contract contracts/raw/mqtt_broker.odcs.yaml \
  --derived-contract contracts/derived/tbl_broker_metrics.odcs.yaml
```

Use `docker logs` to inspect the JSON event stream. If the snapshot path is mounted from the host, restarting the container with changed contracts will emit `added`, `removed`, and `changed` events for the broker and derived contracts.

The container is expected to keep running after startup. This tool is a long-lived MQTT consumer, so an apparently idle container usually means it is waiting for incoming messages and continuing to ingest until you stop it.

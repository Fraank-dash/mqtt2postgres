# Docker

## Build

```bash
docker build -t mqtt2postgres .
```

## Run Against The Local Stack

The default local development workflow is the four-container Compose stack:

```bash
docker compose -f examples/local-stack/docker-compose.yml up --build
```

This starts the publisher, broker, both subscribers, and TimescaleDB together.

The local database service uses `timescale/timescaledb-ha:pg16` so `timescaledb_toolkit` is available during bootstrap.
The publisher service mounts `examples/local-stack/publisher-config.json` read-only and starts with `--config /config/publisher-config.json`.
Publisher `--config` accepts either JSON or YAML settings files with the same structure.
The subscriber service mounts `examples/local-stack/subscriber-config.json` read-only and starts with `--config /config/subscriber-config.json`.
The topic-overview subscriber mounts `examples/local-stack/subscriber-topics-config.json` read-only and starts with `--config /config/subscriber-topics-config.json`.

To generate a learned publisher config from retained aggregates on the host:

```bash
PYTHONPATH=src mqtt2postgres-twin-config \
  --db-host 127.0.0.1 \
  --db-port 55432 \
  --db-name mqtt \
  --db-user postgres \
  --db-password postgres \
  --topic-filter 'sensors/+/temp'
```

## Run A Standalone Ingestor Container

Start the local infrastructure first:

```bash
docker compose -f examples/local-stack/docker-compose.yml up mqtt-broker timescaledb
```

Run the ingestor container:

```bash
docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -e POSTGRES_USERNAME=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -v "$(pwd)/examples/local-stack/subscriber-config.json:/config/subscriber-config.json:ro" \
  mqtt2postgres \
  --config /config/subscriber-config.json
```

## Container Networking

Inside a container, `localhost` points back at the ingestor container itself. For standalone `docker run` against the local host stack, use `host.docker.internal` for both MQTT and TimescaleDB.

## Logs

When running in the foreground, logs are printed in the terminal.

For detached containers:

```bash
docker logs <container-name-or-id>
```

The container is expected to keep running after startup because the tool is a long-lived MQTT consumer.

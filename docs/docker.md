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

This starts the publisher, broker, subscriber, and TimescaleDB together.

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
  mqtt2postgres \
  --log-level INFO \
  --mqtt-host host.docker.internal \
  --mqtt-port 1883 \
  --db-host host.docker.internal \
  --db-port 55432 \
  --db-name mqtt \
  --db-schema public \
  --mqtt-user admin \
  --mqtt-password secret \
  --route 'sensors/+/temp=tbl_sensor_temp' \
  --route '$SYS/broker/messages/#=tbl_broker_metrics'
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

# mqtt2postgres

Minimal CLI tool that subscribes to MQTT topics and writes messages into existing Postgres tables.

## Local environment

Create and activate the conda environment:

```bash
conda env create -f environment.yml
conda activate mqtt2postgres
```

Run the tool through the root wrapper:

```bash
python main.py \
  --db-host 127.0.0.1 \
  --db-name mqtt \
  --db-user postgres \
  --db-password postgres \
  --mqtt-host 127.0.0.1 \
  --mqtt-user admin \
  --mqtt-password secret \
  --map '$SYS/broker/#=tbl_broker_metrics' \
  --map 'sensors/+/temp=tbl_sensor_temp'
```

If you prefer not to pass secrets on the command line, the same values can still come from the environment:

```bash
export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=postgres
export MQTT_USERNAME=admin
export MQTT_PASSWORD=secret
```

## Runtime contract

- Target tables must already exist.
- Each target table must expose `msg_date`, `msg_topic`, and `msg_value`.
- Mappings are evaluated in the order they are passed. The first matching mapping wins.
- MQTT topic filters use normal MQTT wildcard semantics.

## Docker

Build the image:

```bash
docker build -t mqtt2postgres .
```

Run it with the same CLI flags and environment variables you use locally:

```bash
docker run --rm \
  mqtt2postgres \
  --db-host db.example.internal \
  --db-name mqtt \
  --db-user postgres \
  --db-password postgres \
  --mqtt-host broker.example.internal \
  --mqtt-user admin \
  --mqtt-password secret \
  --map '$SYS/broker/#=tbl_broker_metrics'
```

The container is expected to keep running after startup. This tool is a long-lived MQTT consumer, so an apparently idle container usually means it is waiting for incoming messages and continuing to ingest until you stop it.

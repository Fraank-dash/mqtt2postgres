# Configuration

The repo now has two app-specific settings flows:

- subscriber runtime settings under `apps.subscriber`
- publisher simulation settings under `apps.publisher`

Subscriber settings are loaded from a JSON file passed via `--config`, with environment variables used for defaults and secrets.

## Required Database Credentials

```bash
export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=postgres
```

## Connection Defaults

MQTT defaults:

- `mqtt_host`: `127.0.0.1`
- `mqtt_port`: `1883`
- `mqtt_qos`: `0`

Database defaults:

- `db_host`: `127.0.0.1`
- `db_port`: `5432`
- `db_name`: `mqtt`
- `db_schema`: `public`
- `db_ingest_function`: `mqtt_ingest.ingest_message`

Environment variable fallbacks:

- `MQTT_HOST`, `MQTT_PORT`, `MQTT_QOS`
- `MQTT_USERNAME`, `MQTT_PASSWORD`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_SCHEMA`
- `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`
- `MQTT2POSTGRES_DB_INGEST_FUNCTION`

## Topic Filters

Set `topic_filters` in the JSON settings file as an array of MQTT topic filters.

MQTT wildcards follow normal MQTT topic filter rules. The Python subscriber does not map topic filters to tables anymore; it passes each message to the configured database function.

## JSON Config

Use `--config path/to/subscriber.json` to load one subscriber settings file with multiple topic filters:

```json
{
  "mqtt_host": "mqtt-broker",
  "mqtt_port": 1883,
  "db_host": "timescaledb",
  "db_port": 5432,
  "db_name": "mqtt",
  "db_username": "postgres",
  "db_password": "postgres",
  "topic_filters": ["sensors/+/temp", "sensors/+/humidity"],
  "db_ingest_function": "mqtt_ingest.ingest_message",
  "log_format": "json",
  "log_level": "INFO"
}
```

## Database Ingest Function

The default local function is:

```bash
"db_ingest_function": "mqtt_ingest.ingest_message"
```

It receives the MQTT topic, raw payload text, receive timestamp, and metadata JSON. The local TimescaleDB bootstrap stores messages in `mqtt_ingest.messages`.

## Logging

Use `log_format` and `log_level` in the JSON settings file.

`json` is the default log format and works well in Docker. Use `text` for local terminal debugging.

## Publisher Settings

Run the publisher with either direct flags or a JSON/YAML settings file:

```bash
python -m apps.publisher --config path/to/publisher.yaml
```

Publisher settings files can describe multiple publishers and multiple topics per publisher. JSON and YAML use the same document structure.

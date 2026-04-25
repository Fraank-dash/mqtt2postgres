# Configuration

The runtime can be configured with CLI flags, environment variables, or a JSON config file passed via `--config`.

## Required Database Credentials

```bash
export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=postgres
```

Equivalent flags:

```bash
--db-user postgres --db-password postgres
```

## Connection Defaults

MQTT defaults:

- `--mqtt-host`: `127.0.0.1`
- `--mqtt-port`: `1883`
- `--mqtt-qos`: `0`

Database defaults:

- `--db-host`: `127.0.0.1`
- `--db-port`: `5432`
- `--db-name`: `mqtt`
- `--db-schema`: `public`
- `--db-ingest-function`: `mqtt_ingest.ingest_message`

Environment variable fallbacks:

- `MQTT_HOST`, `MQTT_PORT`, `MQTT_QOS`
- `MQTT_USERNAME`, `MQTT_PASSWORD`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_SCHEMA`
- `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`
- `MQTT2POSTGRES_DB_INGEST_FUNCTION`

## Topic Filters

Use one `--topic-filter TOPIC_FILTER` per MQTT subscription:

```bash
--topic-filter 'sensors/+/temp'
```

MQTT wildcards follow normal MQTT topic filter rules. The Python subscriber does not map topic filters to tables anymore; it passes each message to the configured database function.

## JSON Config

Use `--config path/to/subscriber.json` to load one subscriber definition with multiple topic filters:

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

CLI flags override values loaded from `--config`.

## Database Ingest Function

The default local function is:

```bash
--db-ingest-function mqtt_ingest.ingest_message
```

It receives the MQTT topic, raw payload text, receive timestamp, and metadata JSON. The local TimescaleDB bootstrap stores messages in `mqtt_ingest.messages`.

## Logging

```bash
--log-format json
--log-level INFO
```

`json` is the default and works well in Docker. Use `text` for local terminal debugging.

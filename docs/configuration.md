# Configuration

The runtime is configured directly with CLI flags and environment variables. No contract files are required.

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

Environment variable fallbacks:

- `MQTT_HOST`, `MQTT_PORT`, `MQTT_QOS`
- `MQTT_USERNAME`, `MQTT_PASSWORD`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_SCHEMA`
- `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`

## Routes

Use one `--route TOPIC_FILTER=TABLE` per target table:

```bash
--route 'sensors/+/temp=tbl_sensor_temp'
--route '$SYS/broker/messages/#=tbl_broker_metrics'
```

The first matching route wins. MQTT wildcards follow normal MQTT topic filter rules.

## Logging

```bash
--log-format json
--log-level INFO
```

`json` is the default and works well in Docker. Use `text` for local terminal debugging.

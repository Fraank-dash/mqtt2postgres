# Runtime Behavior

## Routing

The app subscribes to all topic filters passed through `--route TOPIC_FILTER=TABLE`.

When a message arrives, the first matching route wins and the message is written into that route's table.

## Database Requirements

Target tables must already exist before the ingestor starts. The local TimescaleDB bootstrap SQL creates:

- `public.tbl_sensor_temp`
- `public.tbl_broker_metrics`

Both local tables are TimescaleDB hypertables partitioned on `msg_date`.

Each target table must include:

- `msg_date`
- `msg_topic`
- `msg_value`

Trace columns such as `event_id`, `trace_id`, `sequence`, `published_at`, `received_at`, and `committed_at` are optional. If present, the writer fills them.

## Logging

The runtime writes logs to stdout/stderr.

- `--log-format json` is the default and is intended for Docker.
- `--log-format text` is intended for local development and debugger sessions.
- `--log-level INFO` is the default verbosity.

Typical event names:

- `service.starting`
- `mqtt.connected`
- `mqtt.subscribed`
- `message.routed`
- `message.unrouted`
- `db.write_succeeded`

MQTT payload bodies are not logged. Logs include metadata such as topic, payload size, target table, status, and error details.

Local verbose run:

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

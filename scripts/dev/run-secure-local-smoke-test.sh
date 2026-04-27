#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"
PYTHON_BIN="${PYTHON_BIN:-python3}"
MQTT_HOST="${MQTT_HOST:-mqtt.pi5.local}"
INGEST_LOG="$ROOT_DIR/.tmp/secure-smoke-ingest.log"
TOPICS_LOG="$ROOT_DIR/.tmp/secure-smoke-topics.log"
INGEST_CONFIG="$ROOT_DIR/.tmp/secure-smoke-ingest.json"
TOPICS_CONFIG="$ROOT_DIR/.tmp/secure-smoke-topics.json"

mkdir -p "$ROOT_DIR/.tmp"

cleanup() {
  if [ -n "${INGEST_PID:-}" ]; then
    kill "$INGEST_PID" >/dev/null 2>&1 || true
    wait "$INGEST_PID" >/dev/null 2>&1 || true
  fi
  if [ -n "${TOPICS_PID:-}" ]; then
    kill "$TOPICS_PID" >/dev/null 2>&1 || true
    wait "$TOPICS_PID" >/dev/null 2>&1 || true
  fi
  docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

if ! getent hosts "$MQTT_HOST" >/dev/null 2>&1; then
  printf 'Secure smoke test requires %s to resolve. Point Technitium DNS at the Docker host IP or override MQTT_HOST.\n' "$MQTT_HOST" >&2
  exit 1
fi

docker compose -f "$COMPOSE_FILE" up -d mqtt-broker timescaledb

until docker compose -f "$COMPOSE_FILE" exec -T timescaledb pg_isready -U postgres -d mqtt >/dev/null 2>&1; do
  sleep 1
done

until [ "$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c \
      "SELECT to_regclass('mqtt_ingest.messages') IS NOT NULL
           AND to_regclass('mqtt_ingest.topic_overview') IS NOT NULL
           AND to_regprocedure('mqtt_ingest.ingest_message(text,text,timestamp with time zone,jsonb)') IS NOT NULL
           AND to_regprocedure('mqtt_ingest.ingest_topics(text,text,timestamp with time zone,jsonb)') IS NOT NULL;" \
      2>/dev/null
)" = "t" ]; do
  sleep 1
done

export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=postgres
export PYTHONPATH="$ROOT_DIR/src"

cat >"$INGEST_CONFIG" <<JSON
{
  "mqtt_host": "$MQTT_HOST",
  "mqtt_port": 1883,
  "mqtt_username": "subscriber-ingest",
  "mqtt_password": "subscriber-ingest-secret",
  "mqtt_client_id": "secure-smoke-ingest",
  "db_host": "127.0.0.1",
  "db_port": 55432,
  "db_name": "mqtt",
  "db_schema": "public",
  "db_username": "postgres",
  "db_password": "postgres",
  "topic_filters": ["sensors/+/temp", "sensors/+/humidity", "sensors/+/power", "sensors/+/energy"],
  "db_ingest_function": "mqtt_ingest.ingest_message",
  "log_format": "text",
  "log_level": "DEBUG"
}
JSON

cat >"$TOPICS_CONFIG" <<JSON
{
  "mqtt_host": "$MQTT_HOST",
  "mqtt_port": 1883,
  "mqtt_username": "subscriber-topics",
  "mqtt_password": "subscriber-topics-secret",
  "mqtt_client_id": "secure-smoke-topics",
  "db_host": "127.0.0.1",
  "db_port": 55432,
  "db_name": "mqtt",
  "db_schema": "public",
  "db_username": "postgres",
  "db_password": "postgres",
  "topic_filters": ["#", "\$SYS/#"],
  "db_ingest_function": "mqtt_ingest.ingest_topics",
  "log_format": "text",
  "log_level": "DEBUG"
}
JSON

"$PYTHON_BIN" -m mqtt2postgres --config "$INGEST_CONFIG" >"$INGEST_LOG" 2>&1 &
INGEST_PID="$!"

"$PYTHON_BIN" -m mqtt2postgres --config "$TOPICS_CONFIG" >"$TOPICS_LOG" 2>&1 &
TOPICS_PID="$!"

sleep 4

"$PYTHON_BIN" "$ROOT_DIR/examples/publish_random.local.py" \
  --host "$MQTT_HOST" \
  --port 1883 \
  --mqtt-username publisher-node-1 \
  --mqtt-password publisher-node-1-secret \
  --topic sensors/node-1/temp \
  --min-value 0 \
  --max-value 10 \
  --frequency-seconds 0.2 \
  --count 2 \
  --seed 7

"$PYTHON_BIN" "$ROOT_DIR/examples/publish_random.local.py" \
  --host "$MQTT_HOST" \
  --port 1883 \
  --mqtt-username publisher-node-2 \
  --mqtt-password publisher-node-2-secret \
  --topic sensors/node-2/humidity \
  --min-value 40 \
  --max-value 60 \
  --frequency-seconds 0.2 \
  --count 2 \
  --seed 8

sleep 3

MESSAGE_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.messages WHERE topic IN ('sensors/node-1/temp', 'sensors/node-2/humidity');"
)"
TOPIC_OVERVIEW_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.topic_overview WHERE topic IN ('sensors/node-1/temp', 'sensors/node-2/humidity');"
)"

SYS_TOPIC_COUNT=0
for _ in 1 2 3 4 5 6 7 8 9 10; do
  SYS_TOPIC_COUNT="$(
    docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
      psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.topic_overview WHERE topic LIKE '\$SYS/%';"
  )"
  if [ "$SYS_TOPIC_COUNT" -gt 0 ]; then
    break
  fi
  sleep 1
done

"$PYTHON_BIN" - <<'PY'
import os
import threading
from paho.mqtt import client as mqtt_client

result = {"rc": None}
event = threading.Event()
host = os.environ["MQTT_HOST"]

def on_connect(client, userdata, flags, rc):
    result["rc"] = rc
    event.set()

client = mqtt_client.Client(client_id="secure-smoke-bad-password", clean_session=True)
client.username_pw_set("publisher-node-1", "wrong-secret")
client.on_connect = on_connect
client.connect(host, 1883)
client.loop_start()
if not event.wait(5):
    client.loop_stop()
    client.disconnect()
    raise SystemExit("Timed out waiting for bad-password connect result.")
client.loop_stop()
client.disconnect()
if result["rc"] == 0:
    raise SystemExit("Expected MQTT authentication failure for wrong password.")
print(f"bad-password-connect-rc={result['rc']}")
PY

"$PYTHON_BIN" - <<'PY'
import os
import threading
import time
from paho.mqtt import client as mqtt_client

result = {"message_count": 0}
connected = threading.Event()
host = os.environ["MQTT_HOST"]

def on_connect(client, userdata, flags, rc):
    if rc != 0:
        raise SystemExit(f"Expected successful connect for ingest subscriber, got rc={rc}")
    connected.set()
    client.subscribe("$SYS/#", qos=0)

def on_message(client, userdata, message):
    result["message_count"] += 1

client = mqtt_client.Client(client_id="secure-smoke-denied-sys", clean_session=True)
client.username_pw_set("subscriber-ingest", "subscriber-ingest-secret")
client.on_connect = on_connect
client.on_message = on_message
client.connect(host, 1883)
client.loop_start()
if not connected.wait(5):
    raise SystemExit("Timed out waiting for ingest subscriber connect.")
time.sleep(3)
client.loop_stop()
client.disconnect()
if result["message_count"] != 0:
    raise SystemExit(f"Expected no SYS messages for ingest subscriber, got {result['message_count']}")
print("denied-sys-delivery=ok")
PY

"$PYTHON_BIN" - <<'PY'
import os
import time
from paho.mqtt import client as mqtt_client

host = os.environ["MQTT_HOST"]
client = mqtt_client.Client(client_id="secure-smoke-denied-publish", clean_session=True)
client.username_pw_set("publisher-node-1", "publisher-node-1-secret")
client.connect(host, 1883)
client.loop_start()
time.sleep(1)
client.publish("sensors/node-2/temp", "99.000000", qos=1)
time.sleep(2)
client.loop_stop()
client.disconnect()
print("denied-publish-attempted")
PY

UNAUTHORIZED_MESSAGE_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.messages WHERE topic = 'sensors/node-2/temp';"
)"
UNAUTHORIZED_TOPIC_OVERVIEW_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.topic_overview WHERE topic = 'sensors/node-2/temp';"
)"

printf 'Secure smoke test inserted %s authenticated sensor rows\n' "$MESSAGE_COUNT"
printf 'Secure smoke test observed %s topic_overview rows for published topics\n' "$TOPIC_OVERVIEW_COUNT"
printf 'Secure smoke test observed %s broker SYS topics\n' "$SYS_TOPIC_COUNT"
printf 'Secure smoke test observed %s unauthorized raw rows and %s unauthorized topic_overview rows\n' "$UNAUTHORIZED_MESSAGE_COUNT" "$UNAUTHORIZED_TOPIC_OVERVIEW_COUNT"

if [ "$MESSAGE_COUNT" -lt 4 ]; then
  printf 'Secure smoke test failed. Expected authenticated ingest rows. Ingest log follows:\n' >&2
  cat "$INGEST_LOG" >&2
  exit 1
fi

if [ "$TOPIC_OVERVIEW_COUNT" -lt 2 ]; then
  printf 'Secure smoke test failed. Topic overview did not capture the published topics. Topics log follows:\n' >&2
  cat "$TOPICS_LOG" >&2
  exit 1
fi

if [ "$SYS_TOPIC_COUNT" -lt 1 ]; then
  printf 'Secure smoke test failed. No $SYS topics were captured by the overview subscriber. Topics log follows:\n' >&2
  cat "$TOPICS_LOG" >&2
  exit 1
fi

if [ "$UNAUTHORIZED_MESSAGE_COUNT" -ne 0 ] || [ "$UNAUTHORIZED_TOPIC_OVERVIEW_COUNT" -ne 0 ]; then
  printf 'Secure smoke test failed. Unauthorized publish reached ingest storage or topic overview.\n' >&2
  exit 1
fi

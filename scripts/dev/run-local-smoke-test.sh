#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"
APP_LOG="$ROOT_DIR/.tmp/local-smoke-ingestor.log"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$ROOT_DIR/.tmp"

cleanup() {
  if [ -n "${APP_PID:-}" ]; then
    kill "$APP_PID" >/dev/null 2>&1 || true
    wait "$APP_PID" >/dev/null 2>&1 || true
  fi
  docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up -d mqtt-broker timescaledb

until docker compose -f "$COMPOSE_FILE" exec -T timescaledb pg_isready -U postgres -d mqtt >/dev/null 2>&1; do
  sleep 1
done

until [ "$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c \
      "SELECT to_regclass('mqtt_ingest.messages') IS NOT NULL
           AND to_regclass('mqtt_ingest.message_3m_aggregates') IS NOT NULL
           AND to_regclass('mqtt_ingest.message_15m_aggregates') IS NOT NULL
           AND to_regclass('mqtt_ingest.message_60m_aggregates') IS NOT NULL
           AND to_regclass('mqtt_ingest.message_24h_aggregates') IS NOT NULL;" \
      2>/dev/null
)" = "t" ]; do
  sleep 1
done

export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=postgres
export PYTHONPATH="$ROOT_DIR/src"

"$PYTHON_BIN" "$ROOT_DIR/main.py" \
  --log-format text \
  --log-level DEBUG \
  --mqtt-host 127.0.0.1 \
  --mqtt-port 1883 \
  --db-host 127.0.0.1 \
  --db-port 55432 \
  --db-name mqtt \
  --topic-filter 'sensors/+/temp' \
  --db-ingest-function mqtt_ingest.ingest_message \
  >"$APP_LOG" 2>&1 &
APP_PID="$!"

sleep 3

"$PYTHON_BIN" "$ROOT_DIR/examples/publish_random.local.py" \
  --host 127.0.0.1 \
  --port 1883 \
  --topic sensors/node-1/temp \
  --min-value 0 \
  --max-value 10 \
  --frequency-seconds 0.2 \
  --count 3 \
  --seed 7

"$PYTHON_BIN" "$ROOT_DIR/examples/publish_random.local.py" \
  --host 127.0.0.1 \
  --port 1883 \
  --topic sensors/node-2/temp \
  --min-value 10 \
  --max-value 20 \
  --frequency-seconds 0.2 \
  --count 2 \
  --seed 9

sleep 2

ROW_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.messages;"
)"
AGGREGATE_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.message_3m_aggregates;"
)"
AGGREGATE_15M_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.message_15m_aggregates;"
)"
AGGREGATE_60M_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.message_60m_aggregates;"
)"
AGGREGATE_24H_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.message_24h_aggregates;"
)"
RAW_DEVICE_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(DISTINCT device_id) FROM mqtt_ingest.messages WHERE metric_name = 'temp';"
)"
AGGREGATE_DEVICE_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(DISTINCT device_id) FROM mqtt_ingest.message_3m_aggregates WHERE metric_name = 'temp';"
)"

printf 'Smoke test inserted %s rows into mqtt_ingest.messages\n' "$ROW_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.message_3m_aggregates\n' "$AGGREGATE_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.message_15m_aggregates\n' "$AGGREGATE_15M_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.message_60m_aggregates\n' "$AGGREGATE_60M_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.message_24h_aggregates\n' "$AGGREGATE_24H_COUNT"
printf 'Smoke test observed %s raw temp devices and %s aggregated temp devices\n' "$RAW_DEVICE_COUNT" "$AGGREGATE_DEVICE_COUNT"

if [ "$ROW_COUNT" -lt 5 ]; then
  printf 'Smoke test failed. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

if [ "$AGGREGATE_COUNT" -lt 1 ]; then
  printf 'Smoke test failed. No aggregate rows were created. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

if [ "$AGGREGATE_15M_COUNT" -lt 1 ] || [ "$AGGREGATE_60M_COUNT" -lt 1 ] || [ "$AGGREGATE_24H_COUNT" -lt 1 ]; then
  printf 'Smoke test failed. Longer aggregate rows were not created. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

if [ "$RAW_DEVICE_COUNT" -lt 2 ] || [ "$AGGREGATE_DEVICE_COUNT" -lt 2 ]; then
  printf 'Smoke test failed. Multi-device temp aggregation was not observed. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

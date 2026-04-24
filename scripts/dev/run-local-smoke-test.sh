#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"
APP_LOG="$ROOT_DIR/.tmp/local-smoke-ingestor.log"
SNAPSHOT_PATH="$ROOT_DIR/.tmp/local-smoke-snapshot.json"

mkdir -p "$ROOT_DIR/.tmp"

cleanup() {
  if [ -n "${APP_PID:-}" ]; then
    kill "$APP_PID" >/dev/null 2>&1 || true
    wait "$APP_PID" >/dev/null 2>&1 || true
  fi
  docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up -d

until docker compose -f "$COMPOSE_FILE" exec -T postgres pg_isready -U postgres -d mqtt >/dev/null 2>&1; do
  sleep 1
done

export DATACONTRACT_POSTGRES_USERNAME=postgres
export DATACONTRACT_POSTGRES_PASSWORD=postgres
export PYTHONPATH="$ROOT_DIR/src"

python "$ROOT_DIR/main.py" \
  --log-format text \
  --log-level DEBUG \
  --config-snapshot-path "$SNAPSHOT_PATH" \
  --broker-contract "$ROOT_DIR/examples/contracts.local/raw/mqtt_broker.odcs.yaml" \
  --derived-contract "$ROOT_DIR/examples/contracts.local/derived/tbl_sensor_temp.odcs.yaml" \
  --derived-contract "$ROOT_DIR/examples/contracts.local/derived/tbl_broker_metrics.odcs.yaml" \
  >"$APP_LOG" 2>&1 &
APP_PID="$!"

sleep 3

python "$ROOT_DIR/examples/publish_random.local.py" \
  --host 127.0.0.1 \
  --port 1883 \
  --topic sensors/node-1/temp \
  --min-value 0 \
  --max-value 10 \
  --frequency-seconds 0.2 \
  --count 5 \
  --seed 7

sleep 2

ROW_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM public.tbl_sensor_temp;"
)"

printf 'Smoke test inserted %s rows into public.tbl_sensor_temp\n' "$ROW_COUNT"

if [ "$ROW_COUNT" -lt 5 ]; then
  printf 'Smoke test failed. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

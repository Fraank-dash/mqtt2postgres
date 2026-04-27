#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"
APP_LOG="$ROOT_DIR/.tmp/local-smoke-ingestor.log"
SUBSCRIBER_CONFIG="$ROOT_DIR/.tmp/local-smoke-subscriber.json"
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
           AND to_regclass('mqtt_ingest.message_24h_aggregates') IS NOT NULL
           AND to_regclass('mqtt_ingest.power_energy_3m_reconciliation') IS NOT NULL
           AND to_regclass('mqtt_ingest.power_energy_15m_reconciliation') IS NOT NULL
           AND to_regclass('mqtt_ingest.power_energy_60m_reconciliation') IS NOT NULL
           AND to_regclass('mqtt_ingest.power_energy_24h_reconciliation') IS NOT NULL
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'message_3m_aggregates'
                   AND column_name = 'numeric_median'
             )
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'message_24h_aggregates'
                   AND column_name = 'numeric_p75'
             )
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'message_3m_aggregates'
                   AND column_name = 'numeric_stderr'
             )
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'message_24h_aggregates'
                   AND column_name = 'numeric_ci95_upper'
             )
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'message_3m_aggregates'
                   AND column_name = 'quality_score'
             )
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'message_24h_aggregates'
                   AND column_name = 'quality_status'
             )
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'message_3m_aggregates'
                   AND column_name = 'interval_gap_cv'
             )
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'message_24h_aggregates'
                   AND column_name = 'quality_interval_score'
             )
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'power_energy_3m_reconciliation'
                   AND column_name = 'power_locf_integral_ws'
             )
           AND EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'mqtt_ingest'
                   AND table_name = 'power_energy_3m_reconciliation'
                   AND column_name = 'drift_linear_pct'
             );" \
      2>/dev/null
)" = "t" ]; do
  sleep 1
done

export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=postgres
export PYTHONPATH="$ROOT_DIR/src"

cat >"$SUBSCRIBER_CONFIG" <<'JSON'
{
  "mqtt_host": "127.0.0.1",
  "mqtt_port": 1883,
  "mqtt_username": "subscriber-ingest",
  "mqtt_password": "subscriber-ingest-secret",
  "mqtt_client_id": "local-smoke-subscriber",
  "db_host": "127.0.0.1",
  "db_port": 55432,
  "db_name": "mqtt",
  "db_schema": "public",
  "db_username": "postgres",
  "db_password": "postgres",
  "topic_filters": ["sensors/+/temp", "sensors/+/power", "sensors/+/energy"],
  "db_ingest_function": "mqtt_ingest.ingest_message",
  "log_format": "text",
  "log_level": "DEBUG"
}
JSON

"$PYTHON_BIN" -m mqtt2postgres --config "$SUBSCRIBER_CONFIG" \
  >"$APP_LOG" 2>&1 &
APP_PID="$!"

sleep 3

"$PYTHON_BIN" "$ROOT_DIR/examples/publish_random.local.py" \
  --host 127.0.0.1 \
  --port 1883 \
  --mqtt-username publisher-node-1 \
  --mqtt-password publisher-node-1-secret \
  --topic sensors/node-1/temp \
  --min-value 0 \
  --max-value 10 \
  --frequency-seconds 0.2 \
  --count 3 \
  --seed 7

"$PYTHON_BIN" "$ROOT_DIR/examples/publish_random.local.py" \
  --host 127.0.0.1 \
  --port 1883 \
  --mqtt-username publisher-node-2 \
  --mqtt-password publisher-node-2-secret \
  --topic sensors/node-2/temp \
  --min-value 10 \
  --max-value 20 \
  --frequency-seconds 0.2 \
  --count 2 \
  --seed 9

docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
  psql -U postgres -d mqtt -c \
  "INSERT INTO mqtt_ingest.messages (
      received_at,
      topic,
      device_id,
      metric_name,
      payload,
      numeric_value,
      metadata
   ) VALUES
      ('2026-01-01T00:02:00+00:00', 'sensors/quality-node/temp', 'quality-node', 'temp', '10', 10, '{}'::jsonb),
      ('2026-01-01T00:03:30+00:00', 'sensors/quality-node/temp', 'quality-node', 'temp', '12', 12, '{}'::jsonb),
      ('2026-01-01T00:04:00+00:00', 'sensors/quality-node/temp', 'quality-node', 'temp', '14', 14, '{}'::jsonb),
      ('2026-01-01T00:05:00+00:00', 'sensors/quality-node/temp', 'quality-node', 'temp', '16', 16, '{}'::jsonb),
      ('2026-01-01T00:06:30+00:00', 'sensors/quality-node/temp', 'quality-node', 'temp', '18', 18, '{}'::jsonb);
   SELECT mqtt_ingest.refresh_message_3m_aggregates('2026-01-01T00:03:00+00:00', '2026-01-01T00:03:00+00:00', now());"

docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
  psql -U postgres -d mqtt -c \
  "INSERT INTO mqtt_ingest.messages (
      received_at,
      topic,
      device_id,
      metric_name,
      payload,
      numeric_value,
      metadata
   ) VALUES
      ('2026-01-01T00:02:00+00:00', 'sensors/exact-node/power', 'exact-node', 'power', '10', 10, '{}'::jsonb),
      ('2026-01-01T00:03:00+00:00', 'sensors/exact-node/power', 'exact-node', 'power', '10', 10, '{}'::jsonb),
      ('2026-01-01T00:04:00+00:00', 'sensors/exact-node/power', 'exact-node', 'power', '10', 10, '{}'::jsonb),
      ('2026-01-01T00:06:00+00:00', 'sensors/exact-node/power', 'exact-node', 'power', '10', 10, '{}'::jsonb),
      ('2026-01-01T00:03:00+00:00', 'sensors/exact-node/energy', 'exact-node', 'energy', '1800', 1800, '{}'::jsonb),
      ('2026-01-01T00:06:00+00:00', 'sensors/exact-node/energy', 'exact-node', 'energy', '3600', 3600, '{}'::jsonb),
      ('2026-01-01T00:02:00+00:00', 'sensors/rounded-node/power', 'rounded-node', 'power', '10.4', 10.4, '{}'::jsonb),
      ('2026-01-01T00:03:00+00:00', 'sensors/rounded-node/power', 'rounded-node', 'power', '10.4', 10.4, '{}'::jsonb),
      ('2026-01-01T00:04:00+00:00', 'sensors/rounded-node/power', 'rounded-node', 'power', '10.4', 10.4, '{}'::jsonb),
      ('2026-01-01T00:06:00+00:00', 'sensors/rounded-node/power', 'rounded-node', 'power', '10.4', 10.4, '{}'::jsonb),
      ('2026-01-01T00:03:00+00:00', 'sensors/rounded-node/energy', 'rounded-node', 'energy', '1800', 1800, '{}'::jsonb),
      ('2026-01-01T00:06:00+00:00', 'sensors/rounded-node/energy', 'rounded-node', 'energy', '3600', 3600, '{}'::jsonb),
      ('2026-01-01T00:02:00+00:00', 'sensors/power-only-node/power', 'power-only-node', 'power', '5', 5, '{}'::jsonb),
      ('2026-01-01T00:03:00+00:00', 'sensors/power-only-node/power', 'power-only-node', 'power', '5', 5, '{}'::jsonb),
      ('2026-01-01T00:04:00+00:00', 'sensors/power-only-node/power', 'power-only-node', 'power', '5', 5, '{}'::jsonb),
      ('2026-01-01T00:06:00+00:00', 'sensors/power-only-node/power', 'power-only-node', 'power', '5', 5, '{}'::jsonb),
      ('2026-01-01T00:03:00+00:00', 'sensors/energy-only-node/energy', 'energy-only-node', 'energy', '100', 100, '{}'::jsonb),
      ('2026-01-01T00:06:00+00:00', 'sensors/energy-only-node/energy', 'energy-only-node', 'energy', '400', 400, '{}'::jsonb);
   SELECT mqtt_ingest.refresh_power_energy_3m_reconciliation('2026-01-01T00:03:00+00:00', '2026-01-01T00:03:00+00:00', now());
   SELECT mqtt_ingest.refresh_power_energy_15m_reconciliation('2026-01-01T00:03:00+00:00', '2026-01-01T00:03:00+00:00', now());
   SELECT mqtt_ingest.refresh_power_energy_60m_reconciliation('2026-01-01T00:03:00+00:00', '2026-01-01T00:03:00+00:00', now());
   SELECT mqtt_ingest.refresh_power_energy_24h_reconciliation('2026-01-01T00:03:00+00:00', '2026-01-01T00:03:00+00:00', now());"

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
RECONCILIATION_3M_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.power_energy_3m_reconciliation;"
)"
RECONCILIATION_15M_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.power_energy_15m_reconciliation;"
)"
RECONCILIATION_60M_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.power_energy_60m_reconciliation;"
)"
RECONCILIATION_24H_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.power_energy_24h_reconciliation;"
)"
AGGREGATE_STATS_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.message_3m_aggregates WHERE numeric_median IS NOT NULL AND numeric_p25 IS NOT NULL AND numeric_p75 IS NOT NULL AND numeric_variance_samp IS NOT NULL AND numeric_stddev_samp IS NOT NULL AND numeric_stderr IS NOT NULL AND numeric_ci95_lower IS NOT NULL AND numeric_ci95_upper IS NOT NULL;"
)"
AGGREGATE_QUALITY_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.message_3m_aggregates WHERE topic = 'sensors/quality-node/temp' AND status = 'aggregated' AND quality_status = 'rated' AND quality_score IS NOT NULL AND quality_boundary_score IS NOT NULL AND quality_count_score IS NOT NULL AND quality_stats_score IS NOT NULL AND quality_interval_score IS NOT NULL AND interval_gap_count IS NOT NULL AND interval_gap_avg_seconds IS NOT NULL AND interval_gap_cv IS NOT NULL AND quality_flags IS NOT NULL;"
)"
RAW_DEVICE_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(DISTINCT device_id) FROM mqtt_ingest.messages WHERE metric_name = 'temp';"
)"
AGGREGATE_DEVICE_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(DISTINCT device_id) FROM mqtt_ingest.message_3m_aggregates WHERE metric_name = 'temp';"
)"
RECON_EXACT_ZERO_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.power_energy_3m_reconciliation WHERE device_id = 'exact-node' AND COALESCE(ABS(drift_locf_signed_ws), 0) < 0.000001 AND COALESCE(ABS(drift_linear_signed_ws), 0) < 0.000001;"
)"
RECON_ROUNDED_DRIFT_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.power_energy_3m_reconciliation WHERE device_id = 'rounded-node' AND drift_locf_abs_ws > 0 AND drift_linear_abs_ws > 0 AND drift_locf_pct IS NOT NULL AND drift_linear_pct IS NOT NULL;"
)"
RECON_POWER_ONLY_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.power_energy_3m_reconciliation WHERE device_id = 'power-only-node' AND power_locf_integral_ws IS NOT NULL AND energy_locf_delta_ws IS NULL AND drift_locf_signed_ws IS NULL;"
)"
RECON_ENERGY_ONLY_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.power_energy_3m_reconciliation WHERE device_id = 'energy-only-node' AND energy_locf_delta_ws IS NOT NULL AND power_locf_integral_ws IS NULL AND drift_locf_signed_ws IS NULL;"
)"
RECON_NON_TARGET_COUNT="$(
  docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
    psql -U postgres -d mqtt -At -c "SELECT COUNT(*) FROM mqtt_ingest.power_energy_3m_reconciliation WHERE device_id = 'quality-node';"
)"

printf 'Smoke test inserted %s rows into mqtt_ingest.messages\n' "$ROW_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.message_3m_aggregates\n' "$AGGREGATE_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.message_15m_aggregates\n' "$AGGREGATE_15M_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.message_60m_aggregates\n' "$AGGREGATE_60M_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.message_24h_aggregates\n' "$AGGREGATE_24H_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.power_energy_3m_reconciliation\n' "$RECONCILIATION_3M_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.power_energy_15m_reconciliation\n' "$RECONCILIATION_15M_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.power_energy_60m_reconciliation\n' "$RECONCILIATION_60M_COUNT"
printf 'Smoke test created %s rows in mqtt_ingest.power_energy_24h_reconciliation\n' "$RECONCILIATION_24H_COUNT"
printf 'Smoke test created %s rows with populated trust metrics in mqtt_ingest.message_3m_aggregates\n' "$AGGREGATE_STATS_COUNT"
printf 'Smoke test created %s completed rows with populated quality and interval scoring in mqtt_ingest.message_3m_aggregates\n' "$AGGREGATE_QUALITY_COUNT"
printf 'Smoke test observed %s raw temp devices and %s aggregated temp devices\n' "$RAW_DEVICE_COUNT" "$AGGREGATE_DEVICE_COUNT"
printf 'Smoke test observed %s exact-match reconciliation rows and %s rounded-drift reconciliation rows\n' "$RECON_EXACT_ZERO_COUNT" "$RECON_ROUNDED_DRIFT_COUNT"
printf 'Smoke test observed %s power-only rows and %s energy-only rows in power-energy reconciliation\n' "$RECON_POWER_ONLY_COUNT" "$RECON_ENERGY_ONLY_COUNT"

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

if [ "$RECONCILIATION_3M_COUNT" -lt 1 ] || [ "$RECONCILIATION_15M_COUNT" -lt 1 ] || [ "$RECONCILIATION_60M_COUNT" -lt 1 ] || [ "$RECONCILIATION_24H_COUNT" -lt 1 ]; then
  printf 'Smoke test failed. Power-energy reconciliation rows were not created across all bucket widths. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

if [ "$AGGREGATE_STATS_COUNT" -lt 1 ]; then
  printf 'Smoke test failed. Trust metrics were not populated on aggregate rows. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

if [ "$AGGREGATE_QUALITY_COUNT" -lt 1 ]; then
  printf 'Smoke test failed. Quality scoring was not populated on a completed aggregate row. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

if [ "$RAW_DEVICE_COUNT" -lt 2 ] || [ "$AGGREGATE_DEVICE_COUNT" -lt 2 ]; then
  printf 'Smoke test failed. Multi-device temp aggregation was not observed. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

if [ "$RECON_EXACT_ZERO_COUNT" -lt 1 ] || [ "$RECON_ROUNDED_DRIFT_COUNT" -lt 1 ] || [ "$RECON_POWER_ONLY_COUNT" -lt 1 ] || [ "$RECON_ENERGY_ONLY_COUNT" -lt 1 ] || [ "$RECON_NON_TARGET_COUNT" -ne 0 ]; then
  printf 'Smoke test failed. Power-energy reconciliation assertions did not hold. Ingestor log follows:\n' >&2
  cat "$APP_LOG" >&2
  exit 1
fi

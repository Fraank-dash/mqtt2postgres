#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
  psql -U postgres -d mqtt -c \
  "SELECT bucket_start,
          bucket_end,
          device_id,
          power_topic,
          energy_topic,
          power_numeric_count,
          energy_numeric_count,
          ROUND(power_locf_avg_w::numeric, 6) AS power_locf_avg_w,
          ROUND(power_linear_avg_w::numeric, 6) AS power_linear_avg_w,
          ROUND(power_locf_integral_ws::numeric, 6) AS power_locf_integral_ws,
          ROUND(power_linear_integral_ws::numeric, 6) AS power_linear_integral_ws,
          ROUND(energy_locf_value_at_bucket_start::numeric, 6) AS energy_locf_value_at_bucket_start,
          ROUND(energy_locf_value_at_bucket_end::numeric, 6) AS energy_locf_value_at_bucket_end,
          ROUND(energy_linear_value_at_bucket_start::numeric, 6) AS energy_linear_value_at_bucket_start,
          ROUND(energy_linear_value_at_bucket_end::numeric, 6) AS energy_linear_value_at_bucket_end,
          ROUND(energy_locf_delta_ws::numeric, 6) AS energy_locf_delta_ws,
          ROUND(energy_linear_delta_ws::numeric, 6) AS energy_linear_delta_ws,
          ROUND(drift_locf_signed_ws::numeric, 6) AS drift_locf_signed_ws,
          ROUND(drift_locf_abs_ws::numeric, 6) AS drift_locf_abs_ws,
          ROUND(drift_locf_pct::numeric, 6) AS drift_locf_pct,
          ROUND(drift_linear_signed_ws::numeric, 6) AS drift_linear_signed_ws,
          ROUND(drift_linear_abs_ws::numeric, 6) AS drift_linear_abs_ws,
          ROUND(drift_linear_pct::numeric, 6) AS drift_linear_pct,
          status,
          refreshed_at
     FROM mqtt_ingest.power_energy_3m_reconciliation
 ORDER BY bucket_start DESC, device_id
    LIMIT 20;"

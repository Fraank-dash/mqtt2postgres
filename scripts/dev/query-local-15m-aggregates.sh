#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
  psql -U postgres -d mqtt -c \
  "SELECT bucket_start,
          bucket_end,
          topic,
          device_id,
          metric_name,
          sample_count,
          numeric_count,
          ROUND(numeric_avg::numeric, 6) AS numeric_avg,
          numeric_min,
          numeric_max,
          ROUND(locf_value_at_bucket_start::numeric, 6) AS locf_value_at_bucket_start,
          ROUND(locf_value_at_bucket_end::numeric, 6) AS locf_value_at_bucket_end,
          ROUND(locf_time_weighted_avg::numeric, 6) AS locf_time_weighted_avg,
          ROUND(linear_value_at_bucket_start::numeric, 6) AS linear_value_at_bucket_start,
          ROUND(linear_value_at_bucket_end::numeric, 6) AS linear_value_at_bucket_end,
          ROUND(linear_time_weighted_avg::numeric, 6) AS linear_time_weighted_avg,
          status,
          refreshed_at
     FROM mqtt_ingest.message_15m_aggregates
 ORDER BY bucket_start DESC, device_id, metric_name, topic
    LIMIT 20;"

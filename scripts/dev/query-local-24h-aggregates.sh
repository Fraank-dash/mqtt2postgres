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
          ROUND(numeric_median::numeric, 6) AS numeric_median,
          ROUND(numeric_p25::numeric, 6) AS numeric_p25,
          ROUND(numeric_p75::numeric, 6) AS numeric_p75,
          ROUND(numeric_variance_samp::numeric, 6) AS numeric_variance_samp,
          ROUND(numeric_stddev_samp::numeric, 6) AS numeric_stddev_samp,
          ROUND(numeric_stderr::numeric, 6) AS numeric_stderr,
          ROUND(numeric_ci95_lower::numeric, 6) AS numeric_ci95_lower,
          ROUND(numeric_ci95_upper::numeric, 6) AS numeric_ci95_upper,
          numeric_min,
          numeric_max,
          ROUND(locf_value_at_bucket_start::numeric, 6) AS locf_value_at_bucket_start,
          ROUND(locf_value_at_bucket_end::numeric, 6) AS locf_value_at_bucket_end,
          ROUND(locf_time_weighted_avg::numeric, 6) AS locf_time_weighted_avg,
          ROUND(linear_value_at_bucket_start::numeric, 6) AS linear_value_at_bucket_start,
          ROUND(linear_value_at_bucket_end::numeric, 6) AS linear_value_at_bucket_end,
          ROUND(linear_time_weighted_avg::numeric, 6) AS linear_time_weighted_avg,
          status,
          ROUND(quality_score::numeric, 2) AS quality_score,
          ROUND(quality_boundary_score::numeric, 2) AS quality_boundary_score,
          ROUND(quality_count_score::numeric, 2) AS quality_count_score,
          ROUND(quality_stats_score::numeric, 2) AS quality_stats_score,
          interval_gap_count,
          ROUND(interval_gap_avg_seconds::numeric, 6) AS interval_gap_avg_seconds,
          ROUND(interval_gap_stddev_seconds::numeric, 6) AS interval_gap_stddev_seconds,
          ROUND(interval_gap_cv::numeric, 6) AS interval_gap_cv,
          ROUND(quality_interval_score::numeric, 2) AS quality_interval_score,
          quality_flags,
          quality_status,
          refreshed_at
     FROM mqtt_ingest.message_24h_aggregates
 ORDER BY bucket_start DESC, device_id, metric_name, topic
    LIMIT 20;"

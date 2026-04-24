#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
  psql -U postgres -d mqtt -c \
  "SELECT trace_id,
          sequence,
          msg_topic,
          published_at,
          received_at,
          committed_at,
          ROUND(EXTRACT(EPOCH FROM (received_at - published_at))::numeric * 1000, 3) AS publish_to_receive_ms,
          ROUND(EXTRACT(EPOCH FROM (committed_at - received_at))::numeric * 1000, 3) AS receive_to_commit_ms
     FROM public.tbl_sensor_temp
    WHERE trace_id IS NOT NULL
 ORDER BY published_at DESC
    LIMIT 50;"

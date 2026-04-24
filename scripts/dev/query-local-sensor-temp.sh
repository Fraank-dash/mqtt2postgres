#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
  psql -U postgres -d mqtt -c \
  "SELECT msg_date, msg_topic, msg_value, trace_id, sequence, published_at, received_at, committed_at FROM public.tbl_sensor_temp ORDER BY msg_date DESC LIMIT 20;"

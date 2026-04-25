#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
  psql -U postgres -d mqtt -c \
  "SELECT received_at, topic, numeric_value, trace_id, sequence, published_at, committed_at FROM mqtt_ingest.messages ORDER BY received_at DESC LIMIT 20;"

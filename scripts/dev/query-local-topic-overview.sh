#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" exec -T timescaledb \
  psql -U postgres -d mqtt -c \
  "SELECT topic,
          first_seen_at,
          last_seen_at,
          message_count,
          last_event_id,
          last_trace_id,
          last_publisher_id,
          last_sequence,
          refreshed_at
     FROM mqtt_ingest.topic_overview
 ORDER BY last_seen_at DESC, topic
    LIMIT 50;"

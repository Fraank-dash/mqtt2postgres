#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/examples/local-stack/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" down -v
exec docker compose -f "$COMPOSE_FILE" up --build

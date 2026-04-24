#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
BROKER_NAME="mqtt2postgres-local-broker"
BROKER_PORT="1883"
BROKER_IMAGE="eclipse-mosquitto:2"
BROKER_CONFIG="$ROOT_DIR/examples/mosquitto/mosquitto.conf"

docker rm -f "$BROKER_NAME" >/dev/null 2>&1 || true

exec docker run --rm \
  --name "$BROKER_NAME" \
  -p "$BROKER_PORT:$BROKER_PORT" \
  -v "$BROKER_CONFIG:/mosquitto/config/mosquitto.conf:ro" \
  "$BROKER_IMAGE"

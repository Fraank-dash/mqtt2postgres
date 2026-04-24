#!/usr/bin/env bash
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
OUTPUT_DIR="$ROOT_DIR/.tmp"
OUTPUT_FILE="${1:-$OUTPUT_DIR/mqtt-traffic.pcap}"

mkdir -p "$OUTPUT_DIR"

exec docker run --rm \
  --network host \
  --cap-add NET_ADMIN \
  --cap-add NET_RAW \
  -v "$OUTPUT_DIR:/captures" \
  nicolaka/netshoot \
  tcpdump -i any -w "/captures/$(basename "$OUTPUT_FILE")" tcp port 1883

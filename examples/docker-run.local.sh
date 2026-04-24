docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -v "$(pwd)/examples/contracts.docker.local:/app/examples/contracts.docker.local:ro" \
  -e DATACONTRACT_POSTGRES_USERNAME=postgres \
  -e DATACONTRACT_POSTGRES_PASSWORD=postgres \
  mqtt2postgres \
  --log-level INFO \
  --broker-contract examples/contracts.docker.local/raw/mqtt_broker.odcs.yaml \
  --derived-contract examples/contracts.docker.local/derived/tbl_sensor_temp.odcs.yaml \
  --derived-contract examples/contracts.docker.local/derived/tbl_broker_metrics.odcs.yaml

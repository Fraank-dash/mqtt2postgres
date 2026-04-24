export DATACONTRACT_POSTGRES_USERNAME=postgres
export DATACONTRACT_POSTGRES_PASSWORD=postgres

python main.py \
  --log-format text \
  --log-level DEBUG \
  --config-snapshot-path .tmp/local-config-snapshot.json \
  --broker-contract examples/contracts.local/raw/mqtt_broker.odcs.yaml \
  --derived-contract examples/contracts.local/derived/tbl_sensor_temp.odcs.yaml \
  --derived-contract examples/contracts.local/derived/tbl_broker_metrics.odcs.yaml

from pathlib import Path

import pytest

from mqtt2postgres.contracts import ContractError, load_broker_contract, load_derived_contract


def test_load_broker_contract_success(tmp_path: Path) -> None:
    contract_path = tmp_path / "broker.odcs.yaml"
    contract_path.write_text(
        """
name: Broker Raw
version: 1.0.0
id: urn:mqtt:broker:raw
servers:
  - server: mqtt-prod
    type: custom
    customProperties:
      protocol: mqtt
      host: localhost
      port: 1883
      qos: 0
      topicFilters:
        - devices/+/temp
models:
  mqtt_message:
    fields:
      topic:
        type: text
      payload:
        type: text
      received_at:
        type: timestamp
""".strip()
        + "\n",
        encoding="utf-8",
    )

    contract = load_broker_contract(contract_path)

    assert contract.server.host == "localhost"
    assert contract.server.topic_filters == ("devices/+/temp",)


def test_load_derived_contract_success(tmp_path: Path) -> None:
    contract_path = tmp_path / "derived.odcs.yaml"
    contract_path.write_text(
        """
name: Temperature Aggregate
version: 1.0.0
id: urn:mqtt2postgres:temp
servers:
  - server: postgres-prod
    type: postgres
    host: localhost
    port: 5432
    database: mqtt
    schema: public
models:
  tbl_temperature:
    fields:
      msg_date:
        type: timestamp
      msg_topic:
        type: text
      msg_value:
        type: text
customProperties:
  sourceContractId: urn:mqtt:broker:raw
  sourceTopicFilters:
    - devices/+/temp
""".strip()
        + "\n",
        encoding="utf-8",
    )

    contract = load_derived_contract(contract_path)

    assert contract.server.host == "localhost"
    assert contract.table_name == "tbl_temperature"


def test_load_broker_contract_rejects_missing_topic_filters(tmp_path: Path) -> None:
    contract_path = tmp_path / "broker.odcs.yaml"
    contract_path.write_text(
        """
name: Broker Raw
version: 1.0.0
servers:
  - server: mqtt-prod
    type: custom
    customProperties:
      protocol: mqtt
      host: localhost
      port: 1883
models:
  mqtt_message:
    fields:
      topic:
        type: text
      payload:
        type: text
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="topicFilters"):
        load_broker_contract(contract_path)


def test_load_derived_contract_rejects_missing_source_topic_filters(tmp_path: Path) -> None:
    contract_path = tmp_path / "derived.odcs.yaml"
    contract_path.write_text(
        """
name: Temperature Aggregate
version: 1.0.0
servers:
  - server: postgres-prod
    type: postgres
    host: localhost
    port: 5432
    database: mqtt
    schema: public
models:
  tbl_temperature:
    fields:
      msg_date:
        type: timestamp
      msg_topic:
        type: text
      msg_value:
        type: text
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="sourceTopicFilters"):
        load_derived_contract(contract_path)

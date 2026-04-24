import argparse
from pathlib import Path

import pytest

from mqtt2postgres.config import ConfigError, build_argument_parser, resolve_config


def write_broker_contract(path: Path) -> None:
    path.write_text(
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


def write_derived_contract(path: Path, source_contract_id: str = "urn:mqtt:broker:raw") -> None:
    path.write_text(
        f"""
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
  sourceContractId: {source_contract_id}
  sourceTopicFilters:
    - devices/+/temp
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_parser_requires_contract_arguments() -> None:
    parser = build_argument_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_resolve_config_requires_database_password(tmp_path: Path) -> None:
    broker = tmp_path / "broker.odcs.yaml"
    derived = tmp_path / "derived.odcs.yaml"
    write_broker_contract(broker)
    write_derived_contract(derived)
    args = argparse.Namespace(
        mqtt_user=None,
        mqtt_password=None,
        mqtt_client_id="mqtt2postgres",
        log_format=None,
        log_level=None,
        config_snapshot_path=None,
        broker_contract=str(broker),
        derived_contracts=[str(derived)],
    )

    with pytest.raises(ConfigError, match="DATACONTRACT_POSTGRES_PASSWORD"):
        resolve_config(args, environ={"DATACONTRACT_POSTGRES_USERNAME": "postgres"})


def test_resolve_config_requires_mqtt_password_if_username_is_set(tmp_path: Path) -> None:
    broker = tmp_path / "broker.odcs.yaml"
    derived = tmp_path / "derived.odcs.yaml"
    write_broker_contract(broker)
    write_derived_contract(derived)
    args = argparse.Namespace(
        mqtt_user="mqtt-user",
        mqtt_password=None,
        mqtt_client_id="mqtt2postgres",
        log_format=None,
        log_level=None,
        config_snapshot_path=None,
        broker_contract=str(broker),
        derived_contracts=[str(derived)],
    )

    with pytest.raises(ConfigError, match="mqtt-password"):
        resolve_config(
            args,
            environ={
                "DATACONTRACT_POSTGRES_USERNAME": "postgres",
                "DATACONTRACT_POSTGRES_PASSWORD": "secret",
            },
        )


def test_resolve_config_loads_two_layer_contracts(tmp_path: Path) -> None:
    broker = tmp_path / "broker.odcs.yaml"
    derived = tmp_path / "derived.odcs.yaml"
    write_broker_contract(broker)
    write_derived_contract(derived)
    args = argparse.Namespace(
        mqtt_user=None,
        mqtt_password=None,
        mqtt_client_id="custom-client",
        log_format=None,
        log_level=None,
        config_snapshot_path=None,
        broker_contract=str(broker),
        derived_contracts=[str(derived)],
    )

    config = resolve_config(
        args,
        environ={
            "DATACONTRACT_POSTGRES_USERNAME": "postgres",
            "DATACONTRACT_POSTGRES_PASSWORD": "secret",
        },
    )

    assert config.broker_contract.server.topic_filters == ("devices/+/temp",)
    assert config.derived_contracts[0].table_name == "tbl_temperature"
    assert config.log_format == "json"
    assert config.log_level == "INFO"


def test_resolve_config_rejects_source_contract_mismatch(tmp_path: Path) -> None:
    broker = tmp_path / "broker.odcs.yaml"
    derived = tmp_path / "derived.odcs.yaml"
    write_broker_contract(broker)
    write_derived_contract(derived, source_contract_id="urn:other")
    args = argparse.Namespace(
        mqtt_user=None,
        mqtt_password=None,
        mqtt_client_id="custom-client",
        log_format=None,
        log_level=None,
        config_snapshot_path=None,
        broker_contract=str(broker),
        derived_contracts=[str(derived)],
    )

    with pytest.raises(ConfigError, match="sourceContractId"):
        resolve_config(
            args,
            environ={
                "DATACONTRACT_POSTGRES_USERNAME": "postgres",
                "DATACONTRACT_POSTGRES_PASSWORD": "secret",
            },
        )


def test_resolve_config_uses_logging_environment_defaults(tmp_path: Path) -> None:
    broker = tmp_path / "broker.odcs.yaml"
    derived = tmp_path / "derived.odcs.yaml"
    snapshot = tmp_path / "state" / "snapshot.json"
    write_broker_contract(broker)
    write_derived_contract(derived)
    args = argparse.Namespace(
        mqtt_user=None,
        mqtt_password=None,
        mqtt_client_id="custom-client",
        log_format=None,
        log_level=None,
        config_snapshot_path=None,
        broker_contract=str(broker),
        derived_contracts=[str(derived)],
    )

    config = resolve_config(
        args,
        environ={
            "DATACONTRACT_POSTGRES_USERNAME": "postgres",
            "DATACONTRACT_POSTGRES_PASSWORD": "secret",
            "MQTT2POSTGRES_LOG_LEVEL": "DEBUG",
            "MQTT2POSTGRES_LOG_FORMAT": "json",
            "MQTT2POSTGRES_CONFIG_SNAPSHOT_PATH": str(snapshot),
        },
    )

    assert config.log_level == "DEBUG"
    assert config.log_format == "json"
    assert config.config_snapshot_path == snapshot


def test_resolve_config_accepts_text_log_format(tmp_path: Path) -> None:
    broker = tmp_path / "broker.odcs.yaml"
    derived = tmp_path / "derived.odcs.yaml"
    write_broker_contract(broker)
    write_derived_contract(derived)
    args = argparse.Namespace(
        mqtt_user=None,
        mqtt_password=None,
        mqtt_client_id="custom-client",
        log_format="text",
        log_level="INFO",
        config_snapshot_path=None,
        broker_contract=str(broker),
        derived_contracts=[str(derived)],
    )

    config = resolve_config(
        args,
        environ={
            "DATACONTRACT_POSTGRES_USERNAME": "postgres",
            "DATACONTRACT_POSTGRES_PASSWORD": "secret",
        },
    )

    assert config.log_format == "text"

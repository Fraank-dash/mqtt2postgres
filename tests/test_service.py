from pathlib import Path

from mqtt2postgres.config import AppConfig
from mqtt2postgres.contracts import BrokerContract, BrokerServer, DerivedContract, PostgresServer
from mqtt2postgres.service import topic_matches


def build_broker_contract() -> BrokerContract:
    return BrokerContract(
        path=Path("contracts/raw/broker.odcs.yaml"),
        name="Broker Raw",
        version="1.0.0",
        contract_id="urn:mqtt:broker:raw",
        server=BrokerServer(
            name="mqtt-prod",
            protocol="mqtt",
            host="localhost",
            port=1883,
            qos=0,
            topic_filters=("devices/+/temp", "$SYS/broker/#"),
        ),
        model_name="mqtt_message",
        fields=("topic", "payload", "received_at"),
    )


def build_derived_contract(table_name: str, topic_filter: str) -> DerivedContract:
    return DerivedContract(
        path=Path(f"contracts/derived/{table_name}.odcs.yaml"),
        name=table_name,
        version="1.0.0",
        contract_id=f"urn:mqtt2postgres:{table_name}",
        server=PostgresServer(
            name="postgres-prod",
            host="localhost",
            port=5432,
            database="mqtt",
            schema="public",
        ),
        table_name=table_name,
        fields=("msg_date", "msg_topic", "msg_value"),
        source_contract_id="urn:mqtt:broker:raw",
        source_topic_filters=(topic_filter,),
    )


def build_config() -> AppConfig:
    return AppConfig(
        db_username="postgres",
        db_password="secret",
        mqtt_username=None,
        mqtt_password=None,
        mqtt_client_id="mqtt2postgres",
        broker_contract=build_broker_contract(),
        derived_contracts=(
            build_derived_contract("tbl_temp", "devices/+/temp"),
            build_derived_contract("tbl_sys", "$SYS/broker/#"),
        ),
    )


def test_topic_matches_single_level_wildcard() -> None:
    assert topic_matches("devices/+/temp", "devices/node-1/temp") is True
    assert topic_matches("devices/+/temp", "devices/node-1/humidity") is False


def test_topic_matches_multi_level_wildcard() -> None:
    assert topic_matches("$SYS/broker/#", "$SYS/broker/clients/total") is True
    assert topic_matches("$SYS/broker/#", "$SYS/other") is False

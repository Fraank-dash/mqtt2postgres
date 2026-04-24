from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, MetaData, String, Table, TIMESTAMP

from mqtt2postgres.contracts import DerivedContract, PostgresServer
from mqtt2postgres.db import DatabaseWriter, load_table, validate_table_contract, validate_table_matches_contract


def build_contract() -> DerivedContract:
    return DerivedContract(
        path=Path("contracts/derived/tbl_mqtt.odcs.yaml"),
        name="MQTT Table",
        version="1.0.0",
        contract_id="urn:mqtt2postgres:tbl_mqtt",
        server=PostgresServer(
            name="production",
            host="localhost",
            port=5432,
            database="mqtt",
            schema="public",
        ),
        table_name="tbl_mqtt",
        fields=("msg_date", "msg_topic", "msg_value"),
        source_contract_id="urn:mqtt:broker:raw",
        source_topic_filters=("devices/+/temp",),
    )


def test_validate_table_contract_rejects_missing_columns() -> None:
    with pytest.raises(ValueError, match="msg_topic"):
        validate_table_contract("tbl_mqtt", ("msg_date", "msg_value"))


def test_validate_table_matches_contract_rejects_missing_contract_field() -> None:
    contract = build_contract()

    with pytest.raises(ValueError, match="msg_value"):
        validate_table_matches_contract(contract, ("msg_date", "msg_topic"))


def test_load_table_rejects_unknown_table() -> None:
    contract = build_contract()
    inspector = SimpleNamespace(has_table=lambda table_name, schema=None: False)

    with pytest.raises(ValueError, match="does not exist"):
        load_table(
            engine=object(),
            contract=contract,
            inspector=inspector,
            table_factory=lambda *args, **kwargs: None,
        )


def test_database_writer_builds_insert_statement() -> None:
    metadata = MetaData()
    table = Table(
        "tbl_mqtt",
        metadata,
        Column("msg_date", TIMESTAMP(timezone=True)),
        Column("msg_topic", String),
        Column("msg_value", String),
    )

    writer = DatabaseWriter(
        contract=build_contract(),
        engine=object(),
        table=table,
        connection=object(),
    )

    statement = writer.build_insert(topic="devices/test", payload="42")

    assert statement.compile().params["msg_topic"] == "devices/test"
    assert statement.compile().params["msg_value"] == "42"


def test_database_writer_insert_message_returns_execute_result() -> None:
    class ConnectionStub:
        def __init__(self) -> None:
            self.committed = False

        def execute(self, statement):
            return "result"

        def commit(self) -> None:
            self.committed = True

    metadata = MetaData()
    table = Table(
        "tbl_mqtt",
        metadata,
        Column("msg_date", TIMESTAMP(timezone=True)),
        Column("msg_topic", String),
        Column("msg_value", String),
    )
    connection = ConnectionStub()
    writer = DatabaseWriter(
        contract=build_contract(),
        engine=object(),
        table=table,
        connection=connection,
    )

    result = writer.insert_message(topic="devices/test", payload="42")

    assert result == "result"
    assert connection.committed is True

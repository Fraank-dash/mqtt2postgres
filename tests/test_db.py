from types import SimpleNamespace

import pytest
from sqlalchemy import Column, MetaData, String, Table, TIMESTAMP

from mqtt2postgres.db import DatabaseWriter, load_tables, validate_table_contract


def test_validate_table_contract_rejects_missing_columns() -> None:
    with pytest.raises(ValueError, match="msg_topic"):
        validate_table_contract("tbl_mqtt", ("msg_date", "msg_value"))


def test_load_tables_rejects_unknown_table() -> None:
    inspector = SimpleNamespace(has_table=lambda table_name, schema=None: False)

    with pytest.raises(ValueError, match="does not exist"):
        load_tables(
            engine=object(),
            schema="public",
            table_names=["tbl_missing"],
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
        engine=object(),
        schema="public",
        tables={"tbl_mqtt": table},
        connection=object(),
    )

    statement = writer.build_insert(
        table_name="tbl_mqtt",
        topic="devices/test",
        payload="42",
    )

    assert statement.compile().params["msg_topic"] == "devices/test"
    assert statement.compile().params["msg_value"] == "42"

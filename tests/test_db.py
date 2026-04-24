from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, MetaData, String, Table, TIMESTAMP

from mqtt2postgres.config import Route
from mqtt2postgres.db import DatabaseWriter, load_table, validate_table_columns
from mqtt2postgres.tracing import TraceEnvelope


def build_route() -> Route:
    return Route(topic_filter="devices/+/temp", table_name="tbl_mqtt")


def test_validate_table_columns_rejects_missing_columns() -> None:
    with pytest.raises(ValueError, match="msg_topic"):
        validate_table_columns("tbl_mqtt", ("msg_date", "msg_value"))


def test_load_table_rejects_unknown_table() -> None:
    inspector = SimpleNamespace(has_table=lambda table_name, schema=None: False)

    with pytest.raises(ValueError, match="does not exist"):
        load_table(
            engine=object(),
            table_name="tbl_mqtt",
            schema="public",
            inspector=inspector,
            table_factory=lambda *args, **kwargs: None,
        )


def test_database_writer_builds_insert_statement() -> None:
    table = build_table()
    writer = DatabaseWriter(
        route=build_route(),
        engine=object(),
        table=table,
        connection=object(),
    )

    trace = TraceEnvelope(
        event_id="event-1",
        trace_id="trace-1",
        publisher_id="publisher-1",
        sequence=1,
        published_at=datetime(2026, 4, 24, tzinfo=timezone.utc),
        value="42",
        raw_payload="42",
    )
    statement = writer.build_insert(topic="devices/test", payload="42", trace=trace)

    assert statement.compile().params["msg_topic"] == "devices/test"
    assert statement.compile().params["msg_value"] == "42"
    assert statement.compile().params["trace_id"] == "trace-1"


def test_database_writer_insert_message_returns_execute_result() -> None:
    class ConnectionStub:
        def __init__(self) -> None:
            self.committed = False

        def execute(self, statement):
            return "result"

        def commit(self) -> None:
            self.committed = True

    connection = ConnectionStub()
    writer = DatabaseWriter(
        route=build_route(),
        engine=object(),
        table=build_table(),
        connection=connection,
    )

    result = writer.insert_message(
        topic="devices/test",
        payload="42",
        trace=TraceEnvelope(
            event_id="event-1",
            trace_id="trace-1",
            publisher_id="publisher-1",
            sequence=1,
            published_at=datetime(2026, 4, 24, tzinfo=timezone.utc),
            value="42",
            raw_payload="42",
        ),
    )

    assert result["result"] == "result"
    assert result["committed_at"] is not None
    assert connection.committed is True


def build_table() -> Table:
    metadata = MetaData()
    return Table(
        "tbl_mqtt",
        metadata,
        Column("msg_date", TIMESTAMP(timezone=True)),
        Column("msg_topic", String),
        Column("msg_value", String),
        Column("event_id", String),
        Column("trace_id", String),
        Column("publisher_id", String),
        Column("sequence", String),
        Column("published_at", TIMESTAMP(timezone=True)),
        Column("received_at", TIMESTAMP(timezone=True)),
        Column("committed_at", TIMESTAMP(timezone=True)),
    )

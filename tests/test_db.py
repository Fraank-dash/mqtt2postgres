from datetime import datetime, timezone

import pytest

from apps.subscriber.runtime import DatabaseFunctionWriter, quote_qualified_function_name


def test_quote_qualified_function_name_quotes_schema_and_name() -> None:
    assert quote_qualified_function_name("mqtt_ingest.ingest_message") == (
        '"mqtt_ingest"."ingest_message"'
    )


def test_quote_qualified_function_name_rejects_unsafe_name() -> None:
    with pytest.raises(ValueError, match="identifiers"):
        quote_qualified_function_name("mqtt_ingest.ingest_message;DROP")


def test_database_function_writer_builds_function_call() -> None:
    writer = DatabaseFunctionWriter(
        function_name="mqtt_ingest.ingest_message",
        engine=object(),
        connection=object(),
    )
    received_at = datetime(2026, 4, 24, tzinfo=timezone.utc)

    statement = writer.build_call(
        topic="devices/node-1/temp",
        payload="42",
        received_at=received_at,
        metadata={"trace_id": "trace-1"},
    )

    compiled = statement.compile()
    assert 'SELECT "mqtt_ingest"."ingest_message"' in str(compiled)
    assert compiled.params["topic"] == "devices/node-1/temp"
    assert compiled.params["payload"] == "42"
    assert compiled.params["received_at"] == received_at
    assert compiled.params["metadata"] == '{"trace_id": "trace-1"}'


def test_database_function_writer_insert_message_returns_execute_result() -> None:
    class ConnectionStub:
        def __init__(self) -> None:
            self.committed = False
            self.statement = None

        def execute(self, statement):
            self.statement = statement
            return "result"

        def commit(self) -> None:
            self.committed = True

    connection = ConnectionStub()
    writer = DatabaseFunctionWriter(
        function_name="mqtt_ingest.ingest_message",
        engine=object(),
        connection=connection,
    )

    result = writer.insert_message(
        topic="devices/node-1/temp",
        payload="42",
        metadata={"topic_filter": "devices/+/temp"},
    )

    assert result["result"] == "result"
    assert result["committed_at"] is not None
    assert connection.statement is not None
    assert connection.committed is True


def test_database_function_writer_rolls_back_on_execute_failure() -> None:
    class ConnectionStub:
        def __init__(self) -> None:
            self.rolled_back = False

        def execute(self, statement):
            raise RuntimeError("boom")

        def commit(self) -> None:
            raise AssertionError("commit should not be called")

        def rollback(self) -> None:
            self.rolled_back = True

    connection = ConnectionStub()
    writer = DatabaseFunctionWriter(
        function_name="mqtt_ingest.ingest_topics",
        engine=object(),
        connection=connection,
    )

    with pytest.raises(RuntimeError, match="boom"):
        writer.insert_message(
            topic="sensors/node-1/temp",
            payload="42",
            metadata={"topic_filter": "#"},
        )

    assert connection.rolled_back is True

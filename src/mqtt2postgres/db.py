from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.engine import Connection, Engine, URL
from sqlalchemy.sql import Insert

from mqtt2postgres.config import AppConfig, Route
from mqtt2postgres.tracing import TraceEnvelope

REQUIRED_FIELDS = frozenset({"msg_date", "msg_topic", "msg_value"})


def create_database_engine(config: AppConfig) -> Engine:
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=config.db_username,
        password=config.db_password,
        host=config.db_host,
        port=config.db_port,
        database=config.db_name,
    )
    return create_engine(url, future=True)


def validate_table_columns(table_name: str, column_names: list[str] | tuple[str, ...]) -> None:
    missing_columns = REQUIRED_FIELDS.difference(column_names)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Table '{table_name}' is missing required columns: {missing}.")


def load_table(
    engine: Engine,
    *,
    table_name: str,
    schema: str,
    metadata: MetaData | None = None,
    inspector=None,
    table_factory=Table,
) -> Table:
    metadata = metadata or MetaData()
    inspector = inspector or inspect(engine)

    if not inspector.has_table(table_name, schema=schema):
        raise ValueError(f"Table '{schema}.{table_name}' does not exist.")

    table = table_factory(
        table_name,
        metadata,
        schema=schema,
        autoload_with=engine,
    )
    validate_table_columns(table_name, tuple(table.columns.keys()))
    return table


@dataclass
class DatabaseWriter:
    route: Route
    engine: Engine
    table: Table
    connection: Connection

    @classmethod
    def from_route(
        cls,
        *,
        route: Route,
        config: AppConfig,
    ) -> "DatabaseWriter":
        engine = create_database_engine(config)
        connection = engine.connect()
        table = load_table(engine=engine, table_name=route.table_name, schema=config.db_schema)
        return cls(
            route=route,
            engine=engine,
            table=table,
            connection=connection,
        )

    def build_insert(
        self,
        topic: str,
        payload: str,
        *,
        trace: TraceEnvelope | None = None,
        received_at: datetime | None = None,
        committed_at: datetime | None = None,
    ) -> Insert:
        received_at = received_at or datetime.now(timezone.utc)
        committed_at = committed_at or received_at
        column_names = set(self.table.columns.keys())
        values: dict[str, Any] = {
            "msg_date": received_at,
            "msg_topic": topic,
            "msg_value": payload,
        }
        if trace is not None:
            if "event_id" in column_names:
                values["event_id"] = trace.event_id
            if "trace_id" in column_names:
                values["trace_id"] = trace.trace_id
            if "publisher_id" in column_names:
                values["publisher_id"] = trace.publisher_id
            if "sequence" in column_names:
                values["sequence"] = trace.sequence
            if "published_at" in column_names:
                values["published_at"] = trace.published_at
            if "received_at" in column_names:
                values["received_at"] = received_at
            if "committed_at" in column_names:
                values["committed_at"] = committed_at
        return self.table.insert().values(**values)

    def insert_message(
        self,
        topic: str,
        payload: str,
        trace: TraceEnvelope | None = None,
        received_at: datetime | None = None,
    ) -> Any:
        received_at = received_at or datetime.now(timezone.utc)
        committed_at = datetime.now(timezone.utc)
        statement = self.build_insert(
            topic=topic,
            payload=payload,
            trace=trace,
            received_at=received_at,
            committed_at=committed_at,
        )
        result = self.connection.execute(statement)
        self.connection.commit()
        return {"result": result, "committed_at": committed_at}

    def close(self) -> None:
        self.connection.close()
        self.engine.dispose()

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.engine import Connection, Engine, URL
from sqlalchemy.sql import Insert

from mqtt2postgres.config import AppConfig

REQUIRED_COLUMNS = frozenset({"msg_date", "msg_topic", "msg_value"})


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


def validate_table_contract(table_name: str, column_names: list[str] | tuple[str, ...]) -> None:
    missing_columns = REQUIRED_COLUMNS.difference(column_names)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"Table '{table_name}' is missing required columns: {missing}."
        )


def load_tables(
    engine: Engine,
    schema: str,
    table_names: list[str] | tuple[str, ...],
    metadata: MetaData | None = None,
    inspector=None,
    table_factory=Table,
) -> dict[str, Table]:
    metadata = metadata or MetaData()
    inspector = inspector or inspect(engine)
    tables: dict[str, Table] = {}

    for table_name in sorted(set(table_names)):
        if not inspector.has_table(table_name, schema=schema):
            raise ValueError(f"Table '{schema}.{table_name}' does not exist.")
        table = table_factory(table_name, metadata, schema=schema, autoload_with=engine)
        validate_table_contract(table_name, tuple(table.columns.keys()))
        tables[table_name] = table

    return tables


@dataclass
class DatabaseWriter:
    engine: Engine
    schema: str
    tables: dict[str, Table]
    connection: Connection

    @classmethod
    def from_config(cls, config: AppConfig) -> "DatabaseWriter":
        engine = create_database_engine(config)
        connection = engine.connect()
        tables = load_tables(
            engine=engine,
            schema=config.db_schema,
            table_names=[mapping.table_name for mapping in config.mappings],
        )
        return cls(
            engine=engine,
            schema=config.db_schema,
            tables=tables,
            connection=connection,
        )

    def build_insert(
        self,
        table_name: str,
        topic: str,
        payload: str,
        message_time: datetime | None = None,
    ) -> Insert:
        timestamp = message_time or datetime.now(timezone.utc)
        table = self.tables[table_name]
        return table.insert().values(
            msg_date=timestamp,
            msg_topic=topic,
            msg_value=payload,
        )

    def insert_message(
        self,
        table_name: str,
        topic: str,
        payload: str,
        message_time: datetime | None = None,
    ) -> None:
        statement = self.build_insert(
            table_name=table_name,
            topic=topic,
            payload=payload,
            message_time=message_time,
        )
        self.connection.execute(statement)
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()
        self.engine.dispose()

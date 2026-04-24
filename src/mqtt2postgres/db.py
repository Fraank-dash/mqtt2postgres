from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.engine import Connection, Engine, URL
from sqlalchemy.sql import Insert

from mqtt2postgres.contracts import DerivedContract, REQUIRED_FIELDS


def create_database_engine(contract: DerivedContract, username: str, password: str) -> Engine:
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=username,
        password=password,
        host=contract.server.host,
        port=contract.server.port,
        database=contract.server.database,
    )
    return create_engine(url, future=True)


def validate_table_contract(table_name: str, column_names: list[str] | tuple[str, ...]) -> None:
    missing_columns = REQUIRED_FIELDS.difference(column_names)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Table '{table_name}' is missing required columns: {missing}.")


def validate_table_matches_contract(contract: DerivedContract, column_names: tuple[str, ...]) -> None:
    missing_columns = set(contract.fields).difference(column_names)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"Table '{contract.server.schema}.{contract.table_name}' does not satisfy contract '{contract.path}'. Missing columns: {missing}."
        )


def load_table(
    engine: Engine,
    contract: DerivedContract,
    metadata: MetaData | None = None,
    inspector=None,
    table_factory=Table,
) -> Table:
    metadata = metadata or MetaData()
    inspector = inspector or inspect(engine)

    if not inspector.has_table(contract.table_name, schema=contract.server.schema):
        raise ValueError(
            f"Table '{contract.server.schema}.{contract.table_name}' does not exist."
        )

    table = table_factory(
        contract.table_name,
        metadata,
        schema=contract.server.schema,
        autoload_with=engine,
    )
    column_names = tuple(table.columns.keys())
    validate_table_contract(contract.table_name, column_names)
    validate_table_matches_contract(contract, column_names)
    return table


@dataclass
class DatabaseWriter:
    contract: DerivedContract
    engine: Engine
    table: Table
    connection: Connection

    @classmethod
    def from_contract(
        cls,
        contract: DerivedContract,
        username: str,
        password: str,
    ) -> "DatabaseWriter":
        engine = create_database_engine(contract, username=username, password=password)
        connection = engine.connect()
        table = load_table(engine=engine, contract=contract)
        return cls(
            contract=contract,
            engine=engine,
            table=table,
            connection=connection,
        )

    def build_insert(
        self,
        topic: str,
        payload: str,
        message_time: datetime | None = None,
    ) -> Insert:
        timestamp = message_time or datetime.now(timezone.utc)
        return self.table.insert().values(
            msg_date=timestamp,
            msg_topic=topic,
            msg_value=payload,
        )

    def insert_message(
        self,
        topic: str,
        payload: str,
        message_time: datetime | None = None,
    ) -> None:
        statement = self.build_insert(
            topic=topic,
            payload=payload,
            message_time=message_time,
        )
        self.connection.execute(statement)
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()
        self.engine.dispose()

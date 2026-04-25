from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine, URL
from sqlalchemy.sql.elements import TextClause

from mqtt2postgres.config import AppConfig

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def quote_qualified_function_name(function_name: str) -> str:
    parts = [part.strip() for part in function_name.split(".")]
    if not parts or any(not part for part in parts):
        raise ValueError("Database ingest function must not be empty.")
    if len(parts) > 2:
        raise ValueError("Database ingest function must be either NAME or SCHEMA.NAME.")
    for part in parts:
        if not IDENTIFIER_PATTERN.fullmatch(part):
            raise ValueError(
                "Database ingest function must contain only unquoted SQL identifiers."
            )
    return ".".join(f'"{part}"' for part in parts)


@dataclass
class DatabaseFunctionWriter:
    function_name: str
    engine: Engine
    connection: Connection

    @classmethod
    def from_config(cls, *, config: AppConfig) -> "DatabaseFunctionWriter":
        engine = create_database_engine(config)
        connection = engine.connect()
        return cls(
            function_name=config.db_ingest_function,
            engine=engine,
            connection=connection,
        )

    def build_call(
        self,
        *,
        topic: str,
        payload: str,
        received_at: datetime,
        metadata: Mapping[str, Any],
    ) -> TextClause:
        quoted_function = quote_qualified_function_name(self.function_name)
        return text(
            f"SELECT {quoted_function}(:topic, :payload, :received_at, CAST(:metadata AS jsonb))"
        ).bindparams(
            topic=topic,
            payload=payload,
            received_at=received_at,
            metadata=json.dumps(metadata, sort_keys=True, default=str),
        )

    def insert_message(
        self,
        *,
        topic: str,
        payload: str,
        metadata: Mapping[str, Any],
        received_at: datetime | None = None,
    ) -> dict[str, Any]:
        received_at = received_at or datetime.now(timezone.utc)
        committed_at = datetime.now(timezone.utc)
        statement = self.build_call(
            topic=topic,
            payload=payload,
            received_at=received_at,
            metadata=metadata,
        )
        try:
            result = self.connection.execute(statement)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return {"result": result, "committed_at": committed_at}

    def close(self) -> None:
        self.connection.close()
        self.engine.dispose()

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

DEFAULT_DB_INGEST_FUNCTION = "mqtt_ingest.ingest_message"


class SubscriberSettingsError(ValueError):
    """Raised when subscriber runtime settings are invalid."""


@dataclass(frozen=True)
class SubscriberSettings:
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_client_id: str
    mqtt_qos: int
    db_host: str
    db_port: int
    db_name: str
    db_schema: str
    db_username: str
    db_password: str
    topic_filters: tuple[str, ...]
    db_ingest_function: str
    log_format: str
    log_level: str


class EventEmitter(Protocol):
    def emit(self, event: str, **kwargs) -> None: ...

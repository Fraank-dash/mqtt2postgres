from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from observability.logging import DEFAULT_LOG_FORMAT, DEFAULT_LOG_LEVEL

DEFAULT_DB_INGEST_FUNCTION = "mqtt_ingest.ingest_message"


class ConfigError(ValueError):
    """Raised when runtime configuration is invalid."""


@dataclass(frozen=True)
class AppConfig:
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


def resolve_config(
    config_path: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> AppConfig:
    env = dict(os.environ if environ is None else environ)
    file_config = load_config_file(config_path) if config_path else {}

    mqtt_host = _config_text(file_config, "mqtt_host") or env.get("MQTT_HOST") or "127.0.0.1"
    mqtt_port = _config_int(file_config, "mqtt_port") or _int_env(env, "MQTT_PORT", 1883)
    mqtt_qos = (
        _config_int(file_config, "mqtt_qos")
        if _config_int(file_config, "mqtt_qos") is not None
        else _int_env(env, "MQTT_QOS", 0)
    )
    mqtt_username = _config_text(file_config, "mqtt_username") or env.get("MQTT_USERNAME")
    mqtt_password = _config_text(file_config, "mqtt_password") or env.get("MQTT_PASSWORD")
    if mqtt_username and not mqtt_password:
        raise ConfigError(
            "An MQTT password is required when an MQTT username is configured. Set mqtt_password in the config file or MQTT_PASSWORD."
        )
    if mqtt_password and not mqtt_username:
        raise ConfigError(
            "An MQTT username is required when an MQTT password is configured. Set mqtt_username in the config file or MQTT_USERNAME."
        )

    db_username = _config_text(file_config, "db_username") or env.get("POSTGRES_USERNAME")
    if not db_username:
        raise ConfigError("A database username is required. Set db_username in the config file or POSTGRES_USERNAME.")
    db_password = _config_text(file_config, "db_password") or env.get("POSTGRES_PASSWORD")
    if not db_password:
        raise ConfigError("A database password is required. Set db_password in the config file or POSTGRES_PASSWORD.")

    raw_topic_filters = _config_topic_filters(file_config)
    topic_filters = tuple(parse_topic_filter(raw_filter) for raw_filter in raw_topic_filters)
    if not topic_filters:
        raise ConfigError("At least one topic filter is required. Provide topic_filters in the config file.")

    return AppConfig(
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_password,
        mqtt_client_id=_config_text(file_config, "mqtt_client_id") or "mqtt2postgres",
        mqtt_qos=mqtt_qos,
        db_host=_config_text(file_config, "db_host") or env.get("POSTGRES_HOST") or "127.0.0.1",
        db_port=_config_int(file_config, "db_port") or _int_env(env, "POSTGRES_PORT", 5432),
        db_name=_config_text(file_config, "db_name") or env.get("POSTGRES_DB") or "mqtt",
        db_schema=_config_text(file_config, "db_schema") or env.get("POSTGRES_SCHEMA") or "public",
        db_username=db_username,
        db_password=db_password,
        topic_filters=topic_filters,
        db_ingest_function=_config_text(file_config, "db_ingest_function")
        or env.get("MQTT2POSTGRES_DB_INGEST_FUNCTION")
        or DEFAULT_DB_INGEST_FUNCTION,
        log_format=_config_text(file_config, "log_format") or env.get("MQTT2POSTGRES_LOG_FORMAT") or DEFAULT_LOG_FORMAT,
        log_level=_config_text(file_config, "log_level") or env.get("MQTT2POSTGRES_LOG_LEVEL") or DEFAULT_LOG_LEVEL,
    )


def parse_topic_filter(raw_topic_filter: str) -> str:
    topic_filter = raw_topic_filter.strip()
    if not topic_filter:
        raise ConfigError("Topic filter must not be empty.")
    return topic_filter


def load_config_file(path: str) -> Mapping[str, object]:
    config_path = Path(path)
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"Subscriber config file does not exist: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Subscriber config file is not valid JSON: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigError("Subscriber config root must be a JSON object.")
    return raw


def _config_text(config: Mapping[str, object], key: str) -> str | None:
    value = config.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"Config field '{key}' must be a string.")
    text = value.strip()
    return text or None


def _config_int(config: Mapping[str, object], key: str) -> int | None:
    value = config.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"Config field '{key}' must be an integer.") from exc


def _config_topic_filters(config: Mapping[str, object]) -> list[str]:
    value = config.get("topic_filters")
    if value is None:
        return []
    if not isinstance(value, list):
        raise ConfigError("Config field 'topic_filters' must be an array of strings.")
    filters: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ConfigError("Config field 'topic_filters' must contain only strings.")
        filters.append(item)
    return filters


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    raw_value = env.get(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer.") from exc

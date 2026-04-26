from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Mapping

from apps.subscriber.models import (
    DEFAULT_DB_INGEST_FUNCTION,
    SubscriberSettings,
    SubscriberSettingsError,
)
from observability.logging import DEFAULT_LOG_FORMAT, DEFAULT_LOG_LEVEL


def resolve_subscriber_settings(
    settings_path: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> SubscriberSettings:
    env = dict(os.environ if environ is None else environ)
    file_settings = load_subscriber_settings_file(settings_path) if settings_path else {}

    mqtt_host = _settings_text(file_settings, "mqtt_host") or env.get("MQTT_HOST") or "127.0.0.1"
    mqtt_port = _settings_int(file_settings, "mqtt_port") or _int_env(env, "MQTT_PORT", 1883)
    mqtt_qos = (
        _settings_int(file_settings, "mqtt_qos")
        if _settings_int(file_settings, "mqtt_qos") is not None
        else _int_env(env, "MQTT_QOS", 0)
    )
    mqtt_username = _settings_text(file_settings, "mqtt_username") or env.get("MQTT_USERNAME")
    mqtt_password = _settings_text(file_settings, "mqtt_password") or env.get("MQTT_PASSWORD")
    if mqtt_username and not mqtt_password:
        raise SubscriberSettingsError(
            "An MQTT password is required when an MQTT username is configured. Set mqtt_password in the settings file or MQTT_PASSWORD."
        )
    if mqtt_password and not mqtt_username:
        raise SubscriberSettingsError(
            "An MQTT username is required when an MQTT password is configured. Set mqtt_username in the settings file or MQTT_USERNAME."
        )

    db_username = _settings_text(file_settings, "db_username") or env.get("POSTGRES_USERNAME")
    if not db_username:
        raise SubscriberSettingsError("A database username is required. Set db_username in the settings file or POSTGRES_USERNAME.")
    db_password = _settings_text(file_settings, "db_password") or env.get("POSTGRES_PASSWORD")
    if not db_password:
        raise SubscriberSettingsError("A database password is required. Set db_password in the settings file or POSTGRES_PASSWORD.")

    raw_topic_filters = _settings_topic_filters(file_settings)
    topic_filters = tuple(parse_topic_filter(raw_filter) for raw_filter in raw_topic_filters)
    if not topic_filters:
        raise SubscriberSettingsError("At least one topic filter is required. Provide topic_filters in the settings file.")

    return SubscriberSettings(
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_password,
        mqtt_client_id=_settings_text(file_settings, "mqtt_client_id") or "mqtt2postgres",
        mqtt_qos=mqtt_qos,
        db_host=_settings_text(file_settings, "db_host") or env.get("POSTGRES_HOST") or "127.0.0.1",
        db_port=_settings_int(file_settings, "db_port") or _int_env(env, "POSTGRES_PORT", 5432),
        db_name=_settings_text(file_settings, "db_name") or env.get("POSTGRES_DB") or "mqtt",
        db_schema=_settings_text(file_settings, "db_schema") or env.get("POSTGRES_SCHEMA") or "public",
        db_username=db_username,
        db_password=db_password,
        topic_filters=topic_filters,
        db_ingest_function=_settings_text(file_settings, "db_ingest_function")
        or env.get("MQTT2POSTGRES_DB_INGEST_FUNCTION")
        or DEFAULT_DB_INGEST_FUNCTION,
        log_format=_settings_text(file_settings, "log_format") or env.get("MQTT2POSTGRES_LOG_FORMAT") or DEFAULT_LOG_FORMAT,
        log_level=_settings_text(file_settings, "log_level") or env.get("MQTT2POSTGRES_LOG_LEVEL") or DEFAULT_LOG_LEVEL,
    )


def parse_topic_filter(raw_topic_filter: str) -> str:
    topic_filter = raw_topic_filter.strip()
    if not topic_filter:
        raise SubscriberSettingsError("Topic filter must not be empty.")
    return topic_filter


def load_subscriber_settings_file(path: str) -> Mapping[str, object]:
    settings_path = Path(path)
    try:
        raw = json.loads(settings_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SubscriberSettingsError(f"Subscriber settings file does not exist: {settings_path}") from exc
    except json.JSONDecodeError as exc:
        raise SubscriberSettingsError(f"Subscriber settings file is not valid JSON: {exc}") from exc

    if not isinstance(raw, dict):
        raise SubscriberSettingsError("Subscriber settings root must be a JSON object.")
    return raw


def _settings_text(settings: Mapping[str, object], key: str) -> str | None:
    value = settings.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise SubscriberSettingsError(f"Settings field '{key}' must be a string.")
    text = value.strip()
    return text or None


def _settings_int(settings: Mapping[str, object], key: str) -> int | None:
    value = settings.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SubscriberSettingsError(f"Settings field '{key}' must be an integer.") from exc


def _settings_topic_filters(settings: Mapping[str, object]) -> list[str]:
    value = settings.get("topic_filters")
    if value is None:
        return []
    if not isinstance(value, list):
        raise SubscriberSettingsError("Settings field 'topic_filters' must be an array of strings.")
    filters: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise SubscriberSettingsError("Settings field 'topic_filters' must contain only strings.")
        filters.append(item)
    return filters


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    raw_value = env.get(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError as exc:
        raise SubscriberSettingsError(f"{name} must be an integer.") from exc

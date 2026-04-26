import json
from pathlib import Path

import pytest

from apps.subscriber.models import DEFAULT_DB_INGEST_FUNCTION, SubscriberSettingsError
from apps.subscriber.settings import (
    parse_topic_filter,
    resolve_subscriber_settings,
)


def test_parse_topic_filter_rejects_empty_value() -> None:
    with pytest.raises(SubscriberSettingsError, match="Topic filter"):
        parse_topic_filter(" ")


def test_resolve_config_requires_database_password() -> None:
    config_path = Path(__file__).parent / "fixtures" / "subscriber-defaults.json"
    with pytest.raises(SubscriberSettingsError, match="database password"):
        resolve_subscriber_settings(settings_path=str(config_path), environ={"POSTGRES_USERNAME": "postgres"})


def test_resolve_config_requires_mqtt_password_if_username_is_set() -> None:
    config_path = Path(__file__).parent / "fixtures" / "subscriber-defaults.json"
    with pytest.raises(SubscriberSettingsError, match="mqtt-password"):
        resolve_subscriber_settings(
            settings_path=str(config_path),
            environ={
                "MQTT_USERNAME": "mqtt-user",
                "POSTGRES_USERNAME": "postgres",
                "POSTGRES_PASSWORD": "secret",
            },
        )


def test_resolve_config_loads_topic_filters_and_defaults() -> None:
    config_path = Path(__file__).parent / "fixtures" / "subscriber-defaults.json"
    config = resolve_subscriber_settings(
        settings_path=str(config_path),
        environ={
            "POSTGRES_USERNAME": "postgres",
            "POSTGRES_PASSWORD": "secret",
        },
    )

    assert config.mqtt_host == "127.0.0.1"
    assert config.mqtt_port == 1883
    assert config.db_host == "127.0.0.1"
    assert config.db_port == 5432
    assert config.topic_filters == ("devices/+/temp",)
    assert config.db_ingest_function == DEFAULT_DB_INGEST_FUNCTION
    assert config.log_format == "json"
    assert config.log_level == "INFO"


def test_resolve_config_loads_json_config(tmp_path: Path) -> None:
    config_path = tmp_path / "subscriber.json"
    config_path.write_text(
        json.dumps(
            {
                "mqtt_host": "mqtt-broker",
                "mqtt_port": 1883,
                "mqtt_client_id": "subscriber-a",
                "db_host": "timescaledb",
                "db_port": 5432,
                "db_name": "mqtt",
                "db_schema": "public",
                "db_username": "postgres",
                "db_password": "secret",
                "topic_filters": ["sensors/+/temp", "sensors/+/humidity"],
                "db_ingest_function": "mqtt_ingest.ingest_message",
                "log_format": "json",
                "log_level": "INFO",
            }
        ),
        encoding="utf-8",
    )

    config = resolve_subscriber_settings(settings_path=str(config_path), environ={})

    assert config.mqtt_host == "mqtt-broker"
    assert config.mqtt_client_id == "subscriber-a"
    assert config.topic_filters == ("sensors/+/temp", "sensors/+/humidity")
    assert config.db_username == "postgres"


def test_resolve_config_uses_environment_defaults() -> None:
    config_path = Path(__file__).parent / "fixtures" / "subscriber-defaults.json"
    config = resolve_subscriber_settings(
        settings_path=str(config_path),
        environ={
            "POSTGRES_USERNAME": "postgres",
            "POSTGRES_PASSWORD": "secret",
            "POSTGRES_HOST": "timescaledb",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "mqtt",
            "POSTGRES_SCHEMA": "public",
            "MQTT_HOST": "mqtt-broker",
            "MQTT_PORT": "1883",
            "MQTT_QOS": "1",
            "MQTT2POSTGRES_DB_INGEST_FUNCTION": "custom.ingest",
            "MQTT2POSTGRES_LOG_LEVEL": "DEBUG",
            "MQTT2POSTGRES_LOG_FORMAT": "json",
        },
    )

    assert config.mqtt_host == "mqtt-broker"
    assert config.mqtt_qos == 1
    assert config.db_host == "timescaledb"
    assert config.db_ingest_function == "custom.ingest"
    assert config.log_level == "DEBUG"
    assert config.log_format == "json"


def test_resolve_config_accepts_text_log_format() -> None:
    config_path = Path(__file__).parent / "fixtures" / "subscriber-text-log.json"
    config = resolve_subscriber_settings(
        settings_path=str(config_path),
        environ={
            "POSTGRES_USERNAME": "postgres",
            "POSTGRES_PASSWORD": "secret",
        },
    )

    assert config.log_format == "text"

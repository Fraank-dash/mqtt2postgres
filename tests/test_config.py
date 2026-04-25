import argparse

import pytest

from mqtt2postgres.config import (
    DEFAULT_DB_INGEST_FUNCTION,
    ConfigError,
    build_argument_parser,
    parse_topic_filter,
    resolve_config,
)


def build_args(**overrides) -> argparse.Namespace:
    values = {
        "mqtt_host": None,
        "mqtt_port": None,
        "mqtt_user": None,
        "mqtt_password": None,
        "mqtt_client_id": "mqtt2postgres",
        "mqtt_qos": None,
        "db_host": None,
        "db_port": None,
        "db_name": None,
        "db_schema": None,
        "db_user": None,
        "db_password": None,
        "topic_filter": ["devices/+/temp"],
        "db_ingest_function": None,
        "log_format": None,
        "log_level": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_parser_requires_topic_filter_argument() -> None:
    parser = build_argument_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_parse_topic_filter_rejects_empty_value() -> None:
    with pytest.raises(ConfigError, match="Topic filter"):
        parse_topic_filter(" ")


def test_resolve_config_requires_database_password() -> None:
    with pytest.raises(ConfigError, match="database password"):
        resolve_config(build_args(), environ={"POSTGRES_USERNAME": "postgres"})


def test_resolve_config_requires_mqtt_password_if_username_is_set() -> None:
    with pytest.raises(ConfigError, match="mqtt-password"):
        resolve_config(
            build_args(mqtt_user="mqtt-user"),
            environ={
                "POSTGRES_USERNAME": "postgres",
                "POSTGRES_PASSWORD": "secret",
            },
        )


def test_resolve_config_loads_topic_filters_and_defaults() -> None:
    config = resolve_config(
        build_args(mqtt_client_id="custom-client"),
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


def test_resolve_config_uses_environment_defaults() -> None:
    config = resolve_config(
        build_args(),
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
    config = resolve_config(
        build_args(log_format="text", log_level="INFO"),
        environ={
            "POSTGRES_USERNAME": "postgres",
            "POSTGRES_PASSWORD": "secret",
        },
    )

    assert config.log_format == "text"

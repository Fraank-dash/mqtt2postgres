import argparse

import pytest

from mqtt2postgres.config import ConfigError, build_argument_parser, parse_mapping, resolve_config


def test_parse_mapping_success() -> None:
    mapping = parse_mapping("sensors/+/temp=tbl_temperature")

    assert mapping.topic_pattern == "sensors/+/temp"
    assert mapping.table_name == "tbl_temperature"


@pytest.mark.parametrize(
    "value",
    [
        "missing_separator",
        "=tbl_only",
        "topic_only=",
    ],
)
def test_parse_mapping_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ConfigError):
        parse_mapping(value)


def test_parser_requires_mapping_argument() -> None:
    parser = build_argument_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--db-host",
                "db",
                "--db-name",
                "mqtt",
                "--mqtt-host",
                "broker",
            ]
        )


def test_resolve_config_requires_database_password() -> None:
    args = argparse.Namespace(
        db_host="db",
        db_port=5432,
        db_name="mqtt",
        db_schema="public",
        db_user="postgres",
        db_password=None,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_user=None,
        mqtt_password=None,
        mqtt_client_id="mqtt2postgres",
        qos=0,
        mappings=["topic/#=tbl_mqtt"],
    )

    with pytest.raises(ConfigError, match="db-password"):
        resolve_config(args, environ={})


def test_resolve_config_requires_mqtt_password_if_username_is_set() -> None:
    args = argparse.Namespace(
        db_host="db",
        db_port=5432,
        db_name="mqtt",
        db_schema="public",
        db_user="postgres",
        db_password=None,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_user="mqtt-user",
        mqtt_password=None,
        mqtt_client_id="mqtt2postgres",
        qos=0,
        mappings=["topic/#=tbl_mqtt"],
    )

    with pytest.raises(ConfigError, match="mqtt-password"):
        resolve_config(args, environ={"POSTGRES_PASSWORD": "secret"})


def test_resolve_config_uses_environment_defaults() -> None:
    args = argparse.Namespace(
        db_host="db",
        db_port=5432,
        db_name="mqtt",
        db_schema="public",
        db_user=None,
        db_password=None,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_user=None,
        mqtt_password=None,
        mqtt_client_id="custom-client",
        qos=1,
        mappings=["topic/#=tbl_mqtt", "sensors/+/temp=tbl_temp"],
    )

    config = resolve_config(
        args,
        environ={
            "POSTGRES_USERNAME": "postgres",
            "POSTGRES_PASSWORD": "secret",
        },
    )

    assert config.db_username == "postgres"
    assert config.db_password == "secret"
    assert len(config.mappings) == 2
    assert config.qos == 1


def test_resolve_config_prefers_cli_credentials() -> None:
    args = argparse.Namespace(
        db_host="db",
        db_port=5432,
        db_name="mqtt",
        db_schema="public",
        db_user="cli-user",
        db_password="cli-password",
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_user="mqtt-cli-user",
        mqtt_password="mqtt-cli-password",
        mqtt_client_id="custom-client",
        qos=1,
        mappings=["topic/#=tbl_mqtt"],
    )

    config = resolve_config(
        args,
        environ={
            "POSTGRES_USERNAME": "env-user",
            "POSTGRES_PASSWORD": "env-password",
            "MQTT_USERNAME": "env-mqtt-user",
            "MQTT_PASSWORD": "env-mqtt-password",
        },
    )

    assert config.db_username == "cli-user"
    assert config.db_password == "cli-password"
    assert config.mqtt_username == "mqtt-cli-user"
    assert config.mqtt_password == "mqtt-cli-password"

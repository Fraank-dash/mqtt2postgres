from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Mapping, Sequence


class ConfigError(ValueError):
    """Raised when runtime configuration is invalid."""


@dataclass(frozen=True)
class TopicMapping:
    topic_pattern: str
    table_name: str


@dataclass(frozen=True)
class AppConfig:
    db_host: str
    db_port: int
    db_name: str
    db_schema: str
    db_username: str
    db_password: str
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_client_id: str
    qos: int
    mappings: tuple[TopicMapping, ...]


def parse_mapping(raw_value: str) -> TopicMapping:
    topic_pattern, separator, table_name = raw_value.partition("=")
    if separator == "":
        raise ConfigError(
            f"Invalid mapping '{raw_value}'. Use the format <topic-pattern>=<table-name>."
        )
    topic_pattern = topic_pattern.strip()
    table_name = table_name.strip()
    if not topic_pattern or not table_name:
        raise ConfigError(
            f"Invalid mapping '{raw_value}'. Topic pattern and table name are required."
        )
    return TopicMapping(topic_pattern=topic_pattern, table_name=table_name)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mqtt2postgres",
        description="Subscribe to MQTT topics and store messages in Postgres tables.",
    )
    parser.add_argument("--db-host", required=True, help="Postgres host")
    parser.add_argument("--db-port", type=int, default=5432, help="Postgres port")
    parser.add_argument("--db-name", required=True, help="Postgres database name")
    parser.add_argument(
        "--db-schema",
        default="public",
        help="Postgres schema containing the target tables",
    )
    parser.add_argument(
        "--db-user",
        dest="db_user",
        help="Postgres username. Falls back to POSTGRES_USERNAME.",
    )
    parser.add_argument(
        "--db-password",
        dest="db_password",
        help="Postgres password. Falls back to POSTGRES_PASSWORD.",
    )
    parser.add_argument("--mqtt-host", required=True, help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument(
        "--mqtt-user",
        dest="mqtt_user",
        help="MQTT username. Falls back to MQTT_USERNAME.",
    )
    parser.add_argument(
        "--mqtt-password",
        dest="mqtt_password",
        help="MQTT password. Falls back to MQTT_PASSWORD.",
    )
    parser.add_argument(
        "--mqtt-client-id",
        default="mqtt2postgres",
        help="MQTT client identifier",
    )
    parser.add_argument(
        "--qos",
        type=int,
        default=0,
        choices=(0, 1, 2),
        help="MQTT subscription QoS",
    )
    parser.add_argument(
        "--map",
        dest="mappings",
        action="append",
        required=True,
        help="Topic-to-table mapping in the format <topic-pattern>=<table-name>. Repeat for multiple mappings.",
    )
    return parser


def resolve_config(
    args: argparse.Namespace,
    environ: Mapping[str, str] | None = None,
) -> AppConfig:
    env = dict(os.environ if environ is None else environ)

    db_username = args.db_user or env.get("POSTGRES_USERNAME")
    if not db_username:
        raise ConfigError(
            "A Postgres username is required. Pass --db-user or set POSTGRES_USERNAME."
        )

    db_password = args.db_password or env.get("POSTGRES_PASSWORD")
    if not db_password:
        raise ConfigError(
            "A Postgres password is required. Pass --db-password or set POSTGRES_PASSWORD."
        )

    mqtt_username = args.mqtt_user or env.get("MQTT_USERNAME")
    mqtt_password = args.mqtt_password or env.get("MQTT_PASSWORD")
    if mqtt_username and not mqtt_password:
        raise ConfigError(
            "An MQTT password is required when an MQTT username is configured. Pass --mqtt-password or set MQTT_PASSWORD."
        )
    if mqtt_password and not mqtt_username:
        raise ConfigError(
            "An MQTT username is required when an MQTT password is configured. Pass --mqtt-user or set MQTT_USERNAME."
        )

    mappings = tuple(parse_mapping(value) for value in args.mappings)

    return AppConfig(
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_schema=args.db_schema,
        db_username=db_username,
        db_password=db_password,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_password,
        mqtt_client_id=args.mqtt_client_id,
        qos=args.qos,
        mappings=mappings,
    )


def load_config(
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> AppConfig:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        return resolve_config(args, environ=environ)
    except ConfigError as exc:
        parser.error(str(exc))

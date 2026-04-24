from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Mapping, Sequence

from mqtt2postgres.runtime_logging import DEFAULT_LOG_FORMAT, DEFAULT_LOG_LEVEL


class ConfigError(ValueError):
    """Raised when runtime configuration is invalid."""


@dataclass(frozen=True)
class Route:
    topic_filter: str
    table_name: str


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
    routes: tuple[Route, ...]
    log_format: str
    log_level: str


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mqtt2postgres",
        description="Subscribe to MQTT topics and write matching messages into Postgres-compatible tables.",
    )
    parser.add_argument("--mqtt-host", default=None, help="MQTT broker host.")
    parser.add_argument("--mqtt-port", type=int, default=None, help="MQTT broker port.")
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
        help="MQTT client identifier.",
    )
    parser.add_argument(
        "--mqtt-qos",
        type=int,
        choices=(0, 1, 2),
        default=None,
        help="MQTT subscription QoS.",
    )
    parser.add_argument("--db-host", default=None, help="Postgres-compatible database host.")
    parser.add_argument("--db-port", type=int, default=None, help="Postgres-compatible database port.")
    parser.add_argument("--db-name", default=None, help="Database name.")
    parser.add_argument("--db-schema", default=None, help="Database schema.")
    parser.add_argument(
        "--db-user",
        dest="db_user",
        default=None,
        help="Database username. Falls back to POSTGRES_USERNAME.",
    )
    parser.add_argument(
        "--db-password",
        dest="db_password",
        default=None,
        help="Database password. Falls back to POSTGRES_PASSWORD.",
    )
    parser.add_argument(
        "--route",
        action="append",
        required=True,
        metavar="TOPIC_FILTER=TABLE",
        help="Route matching MQTT topics into a table. Repeat for multiple routes.",
    )
    parser.add_argument(
        "--log-format",
        dest="log_format",
        choices=("json", "text"),
        default=None,
        help="Runtime log format. Defaults to json.",
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default=None,
        help="Runtime log level. Defaults to INFO.",
    )
    return parser


def resolve_config(
    args: argparse.Namespace,
    environ: Mapping[str, str] | None = None,
) -> AppConfig:
    env = dict(os.environ if environ is None else environ)

    mqtt_host = args.mqtt_host or env.get("MQTT_HOST") or "127.0.0.1"
    mqtt_port = args.mqtt_port or _int_env(env, "MQTT_PORT", 1883)
    mqtt_qos = args.mqtt_qos if args.mqtt_qos is not None else _int_env(env, "MQTT_QOS", 0)
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

    db_username = args.db_user or env.get("POSTGRES_USERNAME")
    if not db_username:
        raise ConfigError("A database username is required. Pass --db-user or set POSTGRES_USERNAME.")
    db_password = args.db_password or env.get("POSTGRES_PASSWORD")
    if not db_password:
        raise ConfigError("A database password is required. Pass --db-password or set POSTGRES_PASSWORD.")

    routes = tuple(parse_route(raw_route) for raw_route in args.route)
    if not routes:
        raise ConfigError("At least one --route TOPIC_FILTER=TABLE mapping is required.")

    return AppConfig(
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_password,
        mqtt_client_id=args.mqtt_client_id,
        mqtt_qos=mqtt_qos,
        db_host=args.db_host or env.get("POSTGRES_HOST") or "127.0.0.1",
        db_port=args.db_port or _int_env(env, "POSTGRES_PORT", 5432),
        db_name=args.db_name or env.get("POSTGRES_DB") or "mqtt",
        db_schema=args.db_schema or env.get("POSTGRES_SCHEMA") or "public",
        db_username=db_username,
        db_password=db_password,
        routes=routes,
        log_format=args.log_format or env.get("MQTT2POSTGRES_LOG_FORMAT") or DEFAULT_LOG_FORMAT,
        log_level=args.log_level or env.get("MQTT2POSTGRES_LOG_LEVEL") or DEFAULT_LOG_LEVEL,
    )


def parse_route(raw_route: str) -> Route:
    topic_filter, separator, table_name = raw_route.partition("=")
    if not separator:
        raise ConfigError(f"Route '{raw_route}' must use TOPIC_FILTER=TABLE format.")
    topic_filter = topic_filter.strip()
    table_name = table_name.strip()
    if not topic_filter:
        raise ConfigError(f"Route '{raw_route}' has an empty topic filter.")
    if not table_name:
        raise ConfigError(f"Route '{raw_route}' has an empty table name.")
    return Route(topic_filter=topic_filter, table_name=table_name)


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    raw_value = env.get(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer.") from exc


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

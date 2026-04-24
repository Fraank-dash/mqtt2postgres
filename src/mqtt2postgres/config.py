from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from mqtt2postgres.contracts import (
    BrokerContract,
    ContractError,
    DerivedContract,
    load_broker_contract,
    load_derived_contract,
)
from mqtt2postgres.runtime_logging import (
    DEFAULT_LOG_FORMAT,
    DEFAULT_LOG_LEVEL,
    default_snapshot_path,
)


class ConfigError(ValueError):
    """Raised when runtime configuration is invalid."""


@dataclass(frozen=True)
class AppConfig:
    db_username: str
    db_password: str
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_client_id: str
    log_format: str
    log_level: str
    config_snapshot_path: Path
    broker_contract: BrokerContract
    derived_contracts: tuple[DerivedContract, ...]


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mqtt2postgres",
        description="Subscribe to MQTT topics from a broker contract and write messages into Postgres tables defined by ODCS derived contracts.",
    )
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
        "--log-format",
        dest="log_format",
        choices=("json", "text"),
        default=None,
        help="Runtime log format. Use json for Docker and text for local terminal debugging. Defaults to json.",
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default=None,
        help="Runtime log level. Defaults to INFO.",
    )
    parser.add_argument(
        "--config-snapshot-path",
        dest="config_snapshot_path",
        default=None,
        help="Path to the persisted config snapshot used for startup diff logging.",
    )
    parser.add_argument(
        "--broker-contract",
        required=True,
        help="Path to the raw broker ODCS contract.",
    )
    parser.add_argument(
        "--derived-contract",
        dest="derived_contracts",
        action="append",
        required=True,
        help="Path to a derived Postgres ODCS contract. Repeat for multiple outputs.",
    )
    return parser


def resolve_config(
    args: argparse.Namespace,
    environ: Mapping[str, str] | None = None,
) -> AppConfig:
    env = dict(os.environ if environ is None else environ)

    db_username = env.get("DATACONTRACT_POSTGRES_USERNAME") or env.get("POSTGRES_USERNAME")
    if not db_username:
        raise ConfigError(
            "A Postgres username is required. Set DATACONTRACT_POSTGRES_USERNAME."
        )

    db_password = env.get("DATACONTRACT_POSTGRES_PASSWORD") or env.get("POSTGRES_PASSWORD")
    if not db_password:
        raise ConfigError(
            "A Postgres password is required. Set DATACONTRACT_POSTGRES_PASSWORD."
        )

    mqtt_username = args.mqtt_user or env.get("MQTT_USERNAME")
    mqtt_password = args.mqtt_password or env.get("MQTT_PASSWORD")
    log_format = args.log_format or env.get("MQTT2POSTGRES_LOG_FORMAT") or DEFAULT_LOG_FORMAT
    log_level = args.log_level or env.get("MQTT2POSTGRES_LOG_LEVEL") or DEFAULT_LOG_LEVEL
    snapshot_path = Path(
        args.config_snapshot_path
        or env.get("MQTT2POSTGRES_CONFIG_SNAPSHOT_PATH")
        or default_snapshot_path()
    )
    if mqtt_username and not mqtt_password:
        raise ConfigError(
            "An MQTT password is required when an MQTT username is configured. Pass --mqtt-password or set MQTT_PASSWORD."
        )
    if mqtt_password and not mqtt_username:
        raise ConfigError(
            "An MQTT username is required when an MQTT password is configured. Pass --mqtt-user or set MQTT_USERNAME."
        )

    try:
        broker_contract = load_broker_contract(Path(args.broker_contract))
    except ContractError as exc:
        raise ConfigError(str(exc)) from exc

    derived_contracts = []
    for contract_path in args.derived_contracts:
        try:
            contract = load_derived_contract(Path(contract_path))
        except ContractError as exc:
            raise ConfigError(str(exc)) from exc
        if (
            contract.source_contract_id
            and broker_contract.contract_id
            and contract.source_contract_id != broker_contract.contract_id
        ):
            raise ConfigError(
                f"Derived contract '{contract.path}' references sourceContractId '{contract.source_contract_id}', expected '{broker_contract.contract_id}'."
            )
        derived_contracts.append(contract)

    return AppConfig(
        db_username=db_username,
        db_password=db_password,
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_password,
        mqtt_client_id=args.mqtt_client_id,
        log_format=log_format,
        log_level=log_level,
        config_snapshot_path=snapshot_path,
        broker_contract=broker_contract,
        derived_contracts=tuple(derived_contracts),
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

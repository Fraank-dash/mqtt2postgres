from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REQUIRED_FIELDS = frozenset({"msg_date", "msg_topic", "msg_value"})


class ContractError(ValueError):
    """Raised when an ODCS contract cannot be used by the app."""


@dataclass(frozen=True)
class PostgresServer:
    name: str
    host: str
    port: int
    database: str
    schema: str


@dataclass(frozen=True)
class BrokerServer:
    name: str
    protocol: str
    host: str
    port: int
    qos: int
    topic_filters: tuple[str, ...]


@dataclass(frozen=True)
class BrokerContract:
    path: Path
    name: str
    version: str
    contract_id: str | None
    server: BrokerServer
    model_name: str
    fields: tuple[str, ...]


@dataclass(frozen=True)
class DerivedContract:
    path: Path
    name: str
    version: str
    contract_id: str | None
    server: PostgresServer
    table_name: str
    fields: tuple[str, ...]
    source_contract_id: str | None
    source_topic_filters: tuple[str, ...]


def load_broker_contract(contract_path: str | Path) -> BrokerContract:
    path, payload = _load_contract_payload(contract_path)
    name = _require_string(payload, "name", path)
    version = _require_string(payload, "version", path)
    contract_id = _optional_string(payload.get("id"))
    server = _load_broker_server(payload.get("servers"), path)
    model_name, fields = _load_model(payload.get("models"), path)
    if "topic" not in fields or "payload" not in fields:
        raise ContractError(
            f"Model '{model_name}' in '{path}' must define 'topic' and 'payload' fields."
        )

    return BrokerContract(
        path=path,
        name=name,
        version=version,
        contract_id=contract_id,
        server=server,
        model_name=model_name,
        fields=tuple(fields),
    )


def load_derived_contract(contract_path: str | Path) -> DerivedContract:
    path, payload = _load_contract_payload(contract_path)
    name = _require_string(payload, "name", path)
    version = _require_string(payload, "version", path)
    contract_id = _optional_string(payload.get("id"))
    server = _load_postgres_server(payload.get("servers"), path)
    table_name, fields = _load_model(payload.get("models"), path)
    _validate_required_fields(table_name, fields)

    custom_properties = _normalize_custom_properties(payload.get("customProperties"))
    source_contract_id = _optional_string(custom_properties.get("sourceContractId"))
    source_topic_filters = _normalize_string_list(
        custom_properties.get("sourceTopicFilters")
        or custom_properties.get("sourceTopicFilter")
    )
    if not source_topic_filters:
        raise ContractError(
            f"Derived contract '{path}' must define customProperties.sourceTopicFilters."
        )

    return DerivedContract(
        path=path,
        name=name,
        version=version,
        contract_id=contract_id,
        server=server,
        table_name=table_name,
        fields=tuple(fields),
        source_contract_id=source_contract_id,
        source_topic_filters=source_topic_filters,
    )


def _load_contract_payload(contract_path: str | Path) -> tuple[Path, dict[str, Any]]:
    path = Path(contract_path)
    if not path.is_file():
        raise ContractError(f"Contract file '{path}' does not exist.")

    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ContractError(f"Contract file '{path}' is not valid YAML.") from exc

    if not isinstance(payload, dict):
        raise ContractError(f"Contract file '{path}' must contain a YAML object.")
    return path, payload


def _require_string(payload: dict[str, Any], key: str, path: Path) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"Contract file '{path}' must define a non-empty '{key}'.")
    return value.strip()


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _load_servers(raw_servers: Any, path: Path) -> list[tuple[str, dict[str, Any]]]:
    if isinstance(raw_servers, dict) and raw_servers:
        pairs = []
        for server_name, server_payload in raw_servers.items():
            if not isinstance(server_payload, dict):
                raise ContractError(f"Server '{server_name}' in '{path}' must be a YAML object.")
            pairs.append((str(server_name), server_payload))
        return pairs

    if isinstance(raw_servers, list) and raw_servers:
        pairs = []
        for index, server_payload in enumerate(raw_servers):
            if not isinstance(server_payload, dict):
                raise ContractError(f"Server entry {index} in '{path}' must be a YAML object.")
            server_name = server_payload.get("server") or server_payload.get("id")
            if not isinstance(server_name, str) or not server_name.strip():
                raise ContractError(
                    f"Server entry {index} in '{path}' must define 'server' or 'id'."
                )
            pairs.append((server_name.strip(), server_payload))
        return pairs

    raise ContractError(f"Contract file '{path}' must define at least one server.")


def _load_postgres_server(raw_servers: Any, path: Path) -> PostgresServer:
    servers = _load_servers(raw_servers, path)
    if len(servers) != 1:
        raise ContractError(
            f"Contract file '{path}' must define exactly one server in this version."
        )

    server_name, server_payload = servers[0]
    server_type = server_payload.get("type")
    if server_type not in {"postgres", "postgresql"}:
        raise ContractError(
            f"Server '{server_name}' in '{path}' must use type 'postgres'."
        )

    host = _require_server_value(server_payload, "host", server_name, path)
    database = _require_server_value(server_payload, "database", server_name, path)
    schema = _require_server_value(server_payload, "schema", server_name, path)
    port = server_payload.get("port", 5432)
    if not isinstance(port, int):
        raise ContractError(
            f"Server '{server_name}' in '{path}' must define an integer 'port'."
        )

    return PostgresServer(
        name=server_name,
        host=host,
        port=port,
        database=database,
        schema=schema,
    )


def _load_broker_server(raw_servers: Any, path: Path) -> BrokerServer:
    servers = _load_servers(raw_servers, path)
    if len(servers) != 1:
        raise ContractError(
            f"Contract file '{path}' must define exactly one server in this version."
        )

    server_name, server_payload = servers[0]
    if server_payload.get("type") != "custom":
        raise ContractError(
            f"Broker server '{server_name}' in '{path}' must use type 'custom'."
        )

    custom_properties = _normalize_custom_properties(server_payload.get("customProperties"))
    protocol = _string_from_sources(server_payload, custom_properties, "protocol", default="mqtt")
    host = _string_from_sources(server_payload, custom_properties, "host")
    port = _int_from_sources(server_payload, custom_properties, "port", default=1883)
    qos = _int_from_sources(custom_properties, {}, "qos", default=0)
    topic_filters = _normalize_string_list(
        custom_properties.get("topicFilters") or custom_properties.get("topicFilter")
    )
    if not topic_filters:
        raise ContractError(
            f"Broker server '{server_name}' in '{path}' must define customProperties.topicFilters."
        )

    return BrokerServer(
        name=server_name,
        protocol=protocol,
        host=host,
        port=port,
        qos=qos,
        topic_filters=topic_filters,
    )


def _string_from_sources(
    primary: dict[str, Any],
    secondary: dict[str, Any],
    key: str,
    default: str | None = None,
) -> str:
    value = primary.get(key, secondary.get(key, default))
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"Contract is missing required string property '{key}'.")
    return value.strip()


def _int_from_sources(
    primary: dict[str, Any],
    secondary: dict[str, Any],
    key: str,
    default: int | None = None,
) -> int:
    value = primary.get(key, secondary.get(key, default))
    if not isinstance(value, int):
        raise ContractError(f"Contract property '{key}' must be an integer.")
    return value


def _require_server_value(
    server_payload: dict[str, Any], key: str, server_name: str, path: Path
) -> str:
    value = server_payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(
            f"Server '{server_name}' in '{path}' must define a non-empty '{key}'."
        )
    return value.strip()


def _load_model(raw_models: Any, path: Path) -> tuple[str, tuple[str, ...]]:
    if not isinstance(raw_models, dict) or not raw_models:
        raise ContractError(f"Contract file '{path}' must define at least one model.")
    if len(raw_models) != 1:
        raise ContractError(
            f"Contract file '{path}' must define exactly one model in this version."
        )

    model_name, model_payload = next(iter(raw_models.items()))
    if not isinstance(model_payload, dict):
        raise ContractError(f"Model '{model_name}' in '{path}' must be a YAML object.")
    fields_payload = model_payload.get("fields")
    if not isinstance(fields_payload, dict) or not fields_payload:
        raise ContractError(f"Model '{model_name}' in '{path}' must define fields.")

    return model_name, tuple(fields_payload.keys())


def _validate_required_fields(table_name: str, fields: tuple[str, ...]) -> None:
    missing_fields = REQUIRED_FIELDS.difference(fields)
    if missing_fields:
        missing = ", ".join(sorted(missing_fields))
        raise ContractError(
            f"Model '{table_name}' is missing required fields: {missing}."
        )


def _normalize_custom_properties(raw_custom_properties: Any) -> dict[str, Any]:
    if raw_custom_properties is None:
        return {}
    if isinstance(raw_custom_properties, dict):
        return raw_custom_properties
    if isinstance(raw_custom_properties, list):
        normalized: dict[str, Any] = {}
        for item in raw_custom_properties:
            if not isinstance(item, dict):
                continue
            prop = item.get("property")
            if isinstance(prop, str) and prop.strip():
                normalized[prop.strip()] = item.get("value")
        return normalized
    return {}


def _normalize_string_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str) and value.strip():
        return (value.strip(),)
    if isinstance(value, list):
        normalized = []
        for item in value:
            if isinstance(item, str) and item.strip():
                normalized.append(item.strip())
        return tuple(normalized)
    return ()

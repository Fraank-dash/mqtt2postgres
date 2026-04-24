from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mqtt2postgres import __version__
from mqtt2postgres.contracts import BrokerContract, DerivedContract

DEFAULT_LOG_FORMAT = "json"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_SNAPSHOT_PATH = Path("/var/lib/mqtt2postgres/config-snapshot.json")
REDACT_KEYS = frozenset({"payload", "msg_value", "db_password", "mqtt_password", "password"})


@dataclass
class RuntimeEvent:
    event: str
    component: str
    message: str
    level: str = "INFO"
    status: str = "ok"
    timestamp: str | None = None
    run_id: str | None = None
    broker_contract_id: str | None = None
    derived_contract_id: str | None = None
    topic: str | None = None
    table: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp or datetime.now(timezone.utc).isoformat(),
            "level": self.level,
            "event": self.event,
            "component": self.component,
            "message": self.message,
            "run_id": self.run_id,
            "broker_contract_id": self.broker_contract_id,
            "derived_contract_id": self.derived_contract_id,
            "topic": self.topic,
            "table": self.table,
            "status": self.status,
            "details": self.details,
        }


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.max_level


Middleware = Callable[[RuntimeEvent], RuntimeEvent | None]


class EventLogger:
    def __init__(
        self,
        *,
        log_format: str = DEFAULT_LOG_FORMAT,
        log_level: str = DEFAULT_LOG_LEVEL,
        run_id: str | None = None,
        broker_contract: BrokerContract | None = None,
        middlewares: Sequence[Middleware] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.log_format = log_format
        self.log_level = log_level.upper()
        self.run_id = run_id or str(uuid.uuid4())
        self.broker_contract = broker_contract
        self.logger = logger or build_python_logger(self.log_level)
        self.middlewares = tuple(
            middlewares
            or (
                self._context_middleware,
                self._redaction_middleware,
                self._volume_middleware,
                self._sink_middleware,
            )
        )

    def emit(
        self,
        event: str,
        *,
        component: str,
        message: str,
        level: str = "INFO",
        status: str = "ok",
        broker_contract: BrokerContract | None = None,
        broker_contract_id: str | None = None,
        derived_contract: DerivedContract | None = None,
        derived_contract_id: str | None = None,
        topic: str | None = None,
        table: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        runtime_event = RuntimeEvent(
            event=event,
            component=component,
            message=message,
            level=level.upper(),
            status=status,
            broker_contract_id=broker_contract_id
            or (
                (broker_contract or self.broker_contract).contract_id
                if (broker_contract or self.broker_contract)
                else None
            ),
            derived_contract_id=derived_contract_id
            or (derived_contract.contract_id if derived_contract else None),
            topic=topic,
            table=table,
            details=dict(details or {}),
        )

        current: RuntimeEvent | None = runtime_event
        for middleware in self.middlewares:
            if current is None:
                return
            current = middleware(current)

    def _context_middleware(self, event: RuntimeEvent) -> RuntimeEvent:
        event.timestamp = event.timestamp or datetime.now(timezone.utc).isoformat()
        event.run_id = self.run_id
        event.details.setdefault("process_id", os.getpid())
        event.details.setdefault("package_version", __version__)
        return event

    def _redaction_middleware(self, event: RuntimeEvent) -> RuntimeEvent:
        event.details = redact_value(event.details)
        return event

    def _volume_middleware(self, event: RuntimeEvent) -> RuntimeEvent:
        return event

    def _sink_middleware(self, event: RuntimeEvent) -> None:
        if self.log_format == "json":
            payload = json.dumps(event.to_record(), sort_keys=True)
        elif self.log_format == "text":
            payload = render_text_event(event)
        else:
            raise ValueError(f"Unsupported log format '{self.log_format}'.")
        self.logger.log(level_to_int(event.level), payload)
        return None


def build_python_logger(log_level: str) -> logging.Logger:
    logger = logging.getLogger("mqtt2postgres.runtime")
    if logger.handlers:
        logger.setLevel(level_to_int(log_level))
        return logger

    logger.setLevel(level_to_int(log_level))
    logger.propagate = False

    formatter = logging.Formatter("%(message)s")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(MaxLevelFilter(logging.ERROR))
    stdout_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
    return logger


def level_to_int(level: str) -> int:
    resolved = logging.getLevelName(level.upper())
    if isinstance(resolved, int):
        return resolved
    return logging.INFO


def redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            if key in REDACT_KEYS:
                continue
            sanitized[key] = redact_value(item)
        return sanitized
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, tuple):
        return [redact_value(item) for item in value]
    return value


def render_text_event(event: RuntimeEvent) -> str:
    parts = [
        event.timestamp or datetime.now(timezone.utc).isoformat(),
        event.level,
        event.component,
        event.event,
        event.message,
    ]
    context = []
    if event.status:
        context.append(f"status={event.status}")
    if event.topic:
        context.append(f"topic={event.topic}")
    if event.table:
        context.append(f"table={event.table}")
    if event.broker_contract_id:
        context.append(f"broker={event.broker_contract_id}")
    if event.derived_contract_id:
        context.append(f"derived={event.derived_contract_id}")
    if event.details:
        details = ", ".join(
            f"{key}={event.details[key]!r}" for key in sorted(event.details)
        )
        context.append(f"details={{ {details} }}")
    if context:
        parts.append("|")
        parts.append(" ".join(context))
    return " ".join(parts)


def default_snapshot_path() -> Path:
    return DEFAULT_SNAPSHOT_PATH


def load_snapshot(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Snapshot file '{path}' must contain a JSON object.")
    return payload


def save_snapshot(path: Path, snapshot: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2, sort_keys=True)
        handle.write("\n")


def build_config_snapshot(
    broker_contract: BrokerContract,
    derived_contracts: Sequence[DerivedContract],
) -> dict[str, Any]:
    return {
        "broker": serialize_broker_contract(broker_contract),
        "derived_contracts": [
            serialize_derived_contract(contract)
            for contract in sorted(derived_contracts, key=derived_contract_identity)
        ],
    }


def serialize_broker_contract(contract: BrokerContract) -> dict[str, Any]:
    return {
        "contract_id": contract.contract_id,
        "path": str(contract.path),
        "name": contract.name,
        "host": contract.server.host,
        "port": contract.server.port,
        "qos": contract.server.qos,
        "topic_filters": list(contract.server.topic_filters),
    }


def serialize_derived_contract(contract: DerivedContract) -> dict[str, Any]:
    return {
        "contract_id": contract.contract_id,
        "path": str(contract.path),
        "name": contract.name,
        "database": contract.server.database,
        "schema": contract.server.schema,
        "table_name": contract.table_name,
        "source_contract_id": contract.source_contract_id,
        "source_topic_filters": list(contract.source_topic_filters),
    }


def emit_snapshot_events(
    event_logger: EventLogger,
    *,
    current_snapshot: Mapping[str, Any],
    previous_snapshot: Mapping[str, Any] | None,
) -> None:
    current_broker = current_snapshot["broker"]
    previous_broker = previous_snapshot.get("broker") if previous_snapshot else None
    if previous_broker is None:
        event_logger.emit(
            "broker.added",
            component="config",
            message="Loaded broker contract for the first time.",
            broker_contract_id=current_broker.get("contract_id"),
            status="added",
            details=current_broker,
        )
    elif contract_snapshot_identity(previous_broker) != contract_snapshot_identity(current_broker):
        event_logger.emit(
            "broker.removed",
            component="config",
            message="Previous broker contract is no longer active.",
            broker_contract_id=previous_broker.get("contract_id"),
            status="removed",
            details=previous_broker,
        )
        event_logger.emit(
            "broker.added",
            component="config",
            message="Loaded a new broker contract.",
            broker_contract_id=current_broker.get("contract_id"),
            status="added",
            details=current_broker,
        )
    elif previous_broker != current_broker:
        event_logger.emit(
            "broker.changed",
            component="config",
            message="Broker contract changed since the previous snapshot.",
            broker_contract_id=current_broker.get("contract_id"),
            status="changed",
            details={
                "before": previous_broker,
                "after": current_broker,
            },
        )

    previous_derived = {
        contract_snapshot_identity(contract): contract
        for contract in (previous_snapshot or {}).get("derived_contracts", [])
    }
    current_derived = {
        contract_snapshot_identity(contract): contract
        for contract in current_snapshot.get("derived_contracts", [])
    }

    for identity, contract in current_derived.items():
        previous_contract = previous_derived.get(identity)
        if previous_contract is None:
            event_logger.emit(
                "derived_contract.added",
                component="config",
                message="Derived contract added since the previous snapshot.",
                derived_contract_id=contract.get("contract_id"),
                table=contract.get("table_name"),
                status="added",
                details=contract,
            )
        elif previous_contract != contract:
            event_logger.emit(
                "derived_contract.changed",
                component="config",
                message="Derived contract changed since the previous snapshot.",
                derived_contract_id=contract.get("contract_id"),
                table=contract.get("table_name"),
                status="changed",
                details={
                    "before": previous_contract,
                    "after": contract,
                },
            )

    for identity, contract in previous_derived.items():
        if identity not in current_derived:
            event_logger.emit(
                "derived_contract.removed",
                component="config",
                message="Derived contract removed since the previous snapshot.",
                derived_contract_id=contract.get("contract_id"),
                table=contract.get("table_name"),
                status="removed",
                details=contract,
            )


def contract_snapshot_identity(contract: Mapping[str, Any]) -> str:
    return str(contract.get("contract_id") or contract.get("path") or contract.get("table_name"))


def derived_contract_identity(contract: DerivedContract) -> str:
    return str(contract.contract_id or contract.path or contract.table_name)

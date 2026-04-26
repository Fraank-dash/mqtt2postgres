from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

from mqtt2postgres import __version__

DEFAULT_LOG_FORMAT = "json"
DEFAULT_LOG_LEVEL = "INFO"
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
        middlewares: Sequence[Middleware] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.log_format = log_format
        self.log_level = log_level.upper()
        self.run_id = run_id or str(uuid.uuid4())
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
    if event.details:
        details = ", ".join(
            f"{key}={event.details[key]!r}" for key in sorted(event.details)
        )
        context.append(f"details={{ {details} }}")
    if context:
        parts.append("|")
        parts.append(" ".join(context))
    return " ".join(parts)

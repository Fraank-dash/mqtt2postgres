from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

TRACE_COLUMN_NAMES = (
    "event_id",
    "trace_id",
    "publisher_id",
    "sequence",
    "published_at",
    "received_at",
    "committed_at",
)


@dataclass(frozen=True)
class TraceEnvelope:
    event_id: str | None
    trace_id: str | None
    publisher_id: str | None
    sequence: int | None
    published_at: datetime | None
    value: str
    raw_payload: str


def new_trace_id() -> str:
    return str(uuid.uuid4())


def new_event_id() -> str:
    return str(uuid.uuid4())


def build_trace_payload(
    *,
    trace_id: str,
    event_id: str,
    publisher_id: str,
    sequence: int,
    published_at: datetime,
    value: float,
) -> str:
    return json.dumps(
        {
            "event_id": event_id,
            "trace_id": trace_id,
            "publisher_id": publisher_id,
            "sequence": sequence,
            "published_at": published_at.isoformat(),
            "value": value,
        },
        sort_keys=True,
    )


def parse_trace_payload(payload: str) -> TraceEnvelope:
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        return TraceEnvelope(None, None, None, None, None, payload, payload)

    if not isinstance(decoded, dict):
        return TraceEnvelope(None, None, None, None, None, payload, payload)

    sequence = decoded.get("sequence")
    if not isinstance(sequence, int):
        sequence = None
    value = decoded.get("value")
    return TraceEnvelope(
        event_id=_optional_text(decoded.get("event_id")),
        trace_id=_optional_text(decoded.get("trace_id")),
        publisher_id=_optional_text(decoded.get("publisher_id")),
        sequence=sequence,
        published_at=_parse_datetime(decoded.get("published_at")),
        value=payload if value is None else str(value),
        raw_payload=payload,
    )


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed

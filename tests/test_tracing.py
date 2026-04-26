from __future__ import annotations

from datetime import datetime, timezone

from observability.tracing import build_trace_payload, new_event_id, new_trace_id, parse_trace_payload


def test_build_and_parse_trace_payload_round_trip() -> None:
    payload = build_trace_payload(
        trace_id="trace-1",
        event_id="event-1",
        publisher_id="publisher-1",
        sequence=5,
        published_at=datetime(2026, 4, 24, tzinfo=timezone.utc),
        value=4.25,
    )

    envelope = parse_trace_payload(payload)

    assert envelope.trace_id == "trace-1"
    assert envelope.event_id == "event-1"
    assert envelope.publisher_id == "publisher-1"
    assert envelope.sequence == 5
    assert envelope.value == "4.25"


def test_parse_trace_payload_falls_back_for_plain_text() -> None:
    envelope = parse_trace_payload("42")

    assert envelope.trace_id is None
    assert envelope.event_id is None
    assert envelope.value == "42"


def test_trace_ids_are_generated() -> None:
    assert new_trace_id()
    assert new_event_id()

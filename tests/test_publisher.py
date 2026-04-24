from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from mqtt2postgres.publisher import (
    PublisherConfig,
    PublisherError,
    config_from_args,
    create_rng,
    generate_value,
    main,
    publish_messages,
    render_publish_message,
    run_publisher,
    validate_config,
)
from mqtt2postgres.tracing import parse_trace_payload


class FakeClient:
    def __init__(self) -> None:
        self.connect_calls: list[tuple[str, int]] = []
        self.publish_calls: list[tuple[str, str, int]] = []
        self.loop_started = False
        self.loop_stopped = False
        self.disconnected = False

    def connect(self, host: str, port: int) -> None:
        self.connect_calls.append((host, port))

    def loop_start(self) -> None:
        self.loop_started = True

    def loop_stop(self) -> None:
        self.loop_stopped = True

    def disconnect(self) -> None:
        self.disconnected = True

    def publish(self, topic: str, payload: str, qos: int = 0):
        self.publish_calls.append((topic, payload, qos))
        return SimpleNamespace(rc=0)


def build_config(**overrides) -> PublisherConfig:
    base = PublisherConfig(
        host="127.0.0.1",
        port=1883,
        topic="sensors/node-1/temp",
        min_value=0.0,
        max_value=10.0,
        frequency_seconds=0.5,
        count=3,
        client_id="publisher-test",
        publisher_id="publisher-1",
        qos=1,
        seed=7,
        trace_id="trace-1",
    )
    return PublisherConfig(**(base.__dict__ | overrides))


def test_validate_config_rejects_invalid_range() -> None:
    with pytest.raises(PublisherError, match="Minimum value"):
        validate_config(build_config(min_value=10.0, max_value=1.0))


def test_validate_config_rejects_non_positive_frequency() -> None:
    with pytest.raises(PublisherError, match="frequency"):
        validate_config(build_config(frequency_seconds=0))


def test_rng_output_stays_within_requested_range() -> None:
    config = build_config(min_value=2.0, max_value=3.0)
    rng = create_rng(123)

    values = [generate_value(config, rng) for _ in range(20)]

    assert all(2.0 <= value <= 3.0 for value in values)


def test_seeded_rng_is_deterministic() -> None:
    config = build_config(seed=99)

    values_a = [generate_value(config, create_rng(config.seed)) for _ in range(3)]
    values_b = [generate_value(config, create_rng(config.seed)) for _ in range(3)]

    assert values_a == values_b


def test_publish_payload_contains_trace_fields() -> None:
    client = FakeClient()

    publish_messages(
        client,
        build_config(count=1, seed=11, trace_id="trace-123"),
        sleep_fn=lambda _: None,
        emit_line=lambda _: None,
        now_fn=lambda: datetime(2026, 4, 24, tzinfo=timezone.utc),
    )

    envelope = parse_trace_payload(client.publish_calls[0][1])

    assert envelope.trace_id == "trace-123"
    assert envelope.publisher_id == "publisher-1"
    assert envelope.sequence == 1
    assert envelope.event_id is not None
    assert envelope.value is not None


def test_publish_messages_respects_count_limit() -> None:
    client = FakeClient()
    lines: list[str] = []

    published = publish_messages(
        client,
        build_config(count=2),
        sleep_fn=lambda _: None,
        emit_line=lines.append,
        now_fn=lambda: datetime(2026, 4, 24, tzinfo=timezone.utc),
    )

    assert published == 2
    assert len(client.publish_calls) == 2
    assert len(lines) == 2


def test_publish_messages_can_be_interrupted_in_continuous_mode() -> None:
    client = FakeClient()
    lines: list[str] = []

    def stop_after_first(_: float) -> None:
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        publish_messages(
            client,
            build_config(count=None),
            sleep_fn=stop_after_first,
            emit_line=lines.append,
            now_fn=lambda: datetime(2026, 4, 24, tzinfo=timezone.utc),
        )

    assert len(client.publish_calls) == 1
    assert len(lines) == 1


def test_run_publisher_wires_client_parameters() -> None:
    client = FakeClient()
    lines: list[str] = []

    def client_factory(*, client_id: str, clean_session: bool):
        assert client_id == "publisher-test"
        assert clean_session is True
        return client

    published = run_publisher(
        build_config(count=1),
        client_factory=client_factory,
        sleep_fn=lambda _: None,
        emit_line=lines.append,
        now_fn=lambda: datetime(2026, 4, 24, tzinfo=timezone.utc),
    )

    assert published == 1
    assert client.connect_calls == [("127.0.0.1", 1883)]
    assert client.publish_calls[0][0] == "sensors/node-1/temp"
    assert client.publish_calls[0][2] == 1
    assert client.loop_started is True
    assert client.loop_stopped is True
    assert client.disconnected is True


def test_config_from_args_builds_valid_config() -> None:
    args = SimpleNamespace(
        host="127.0.0.1",
        port=1883,
        topic="sensors/node-1/temp",
        min_value=0.0,
        max_value=10.0,
        frequency_seconds=1.0,
        count=5,
        client_id="publisher-test",
        publisher_id=None,
        qos=0,
        seed=11,
        trace_id="trace-123",
    )

    config = config_from_args(args)

    assert config.count == 5
    assert config.seed == 11
    assert config.publisher_id == "publisher-test"
    assert config.trace_id == "trace-123"


def test_render_publish_message_contains_core_fields() -> None:
    rendered = render_publish_message(
        index=2,
        topic="sensors/node-1/temp",
        payload="{\"value\":4.2}",
        timestamp=datetime(2026, 4, 24, tzinfo=timezone.utc),
        event_id="event-1",
        trace_id="trace-1",
    )

    assert "index=2" in rendered
    assert "topic=sensors/node-1/temp" in rendered
    assert "event_id=event-1" in rendered
    assert "trace_id=trace-1" in rendered


def test_main_returns_zero_on_keyboard_interrupt(monkeypatch) -> None:
    monkeypatch.setattr(
        "mqtt2postgres.publisher.run_publisher",
        lambda config: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    exit_code = main(
        [
            "--topic",
            "sensors/node-1/temp",
            "--min-value",
            "0",
            "--max-value",
            "10",
            "--frequency-seconds",
            "1",
        ]
    )

    assert exit_code == 0

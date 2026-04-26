from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from apps.publisher import (
    PublisherConfig,
    PublisherError,
    PublisherTopicConfig,
    config_from_args,
    create_rng,
    generate_value,
    load_publisher_configs,
    main,
    publish_messages,
    render_publish_message,
    run_publisher,
    run_publishers,
    validate_config,
)
from observability.tracing import parse_trace_payload


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


def build_topic_config(**overrides) -> PublisherTopicConfig:
    base = PublisherTopicConfig(
        topic="sensors/node-1/temp",
        min_value=0.0,
        max_value=10.0,
        seed=7,
        trace_id="trace-1",
        kind="uniform",
        mean=None,
        stddev=None,
    )
    return PublisherTopicConfig(**(base.__dict__ | overrides))


def build_config(**overrides) -> PublisherConfig:
    base = PublisherConfig(
        host="127.0.0.1",
        port=1883,
        frequency_seconds=0.5,
        count=3,
        client_id="publisher-test",
        publisher_id="publisher-1",
        qos=1,
        payload_format="json",
        topics=(build_topic_config(),),
    )
    return PublisherConfig(**(base.__dict__ | overrides))


def test_validate_config_rejects_invalid_range() -> None:
    with pytest.raises(PublisherError, match="min_value"):
        validate_config(build_config(topics=(build_topic_config(min_value=10.0, max_value=1.0),)))


def test_validate_config_rejects_non_positive_frequency() -> None:
    with pytest.raises(PublisherError, match="frequency_seconds"):
        validate_config(build_config(frequency_seconds=0))


def test_rng_output_stays_within_requested_range() -> None:
    config = build_topic_config(min_value=2.0, max_value=3.0)
    rng = create_rng(123)

    values = [generate_value(config, rng) for _ in range(20)]

    assert all(2.0 <= value <= 3.0 for value in values)


def test_seeded_rng_is_deterministic() -> None:
    config = build_topic_config(seed=99)

    values_a = [generate_value(config, create_rng(config.seed)) for _ in range(3)]
    values_b = [generate_value(config, create_rng(config.seed)) for _ in range(3)]

    assert values_a == values_b


def test_clipped_normal_output_stays_within_requested_range() -> None:
    config = build_topic_config(
        kind="clipped_normal",
        min_value=2.0,
        max_value=3.0,
        mean=10.0,
        stddev=5.0,
        seed=123,
    )
    rng = create_rng(config.seed)

    values = [generate_value(config, rng) for _ in range(100)]

    assert all(2.0 <= value <= 3.0 for value in values)


def test_validate_config_rejects_non_positive_clipped_normal_stddev() -> None:
    with pytest.raises(PublisherError, match="stddev > 0"):
        validate_config(
            build_config(
                topics=(
                    build_topic_config(
                        kind="clipped_normal",
                        mean=5.0,
                        stddev=0.0,
                    ),
                )
            )
        )


def test_publish_payload_contains_trace_fields() -> None:
    client = FakeClient()

    publish_messages(
        client,
        build_config(count=1),
        sleep_fn=lambda _: None,
        emit_line=lambda _: None,
        now_fn=lambda: datetime(2026, 4, 24, tzinfo=timezone.utc),
    )

    envelope = parse_trace_payload(client.publish_calls[0][1])

    assert envelope.trace_id == "trace-1"
    assert envelope.publisher_id == "publisher-1"
    assert envelope.sequence == 1
    assert envelope.event_id is not None
    assert envelope.value is not None


def test_publish_messages_can_emit_plain_numeric_payloads() -> None:
    client = FakeClient()

    publish_messages(
        client,
        build_config(count=1, payload_format="plain"),
        sleep_fn=lambda _: None,
        emit_line=lambda _: None,
        now_fn=lambda: datetime(2026, 4, 24, tzinfo=timezone.utc),
    )

    payload = client.publish_calls[0][1]
    assert payload.count(".") == 1
    assert 0 <= float(payload) <= 10


def test_publish_messages_respects_count_limit() -> None:
    client = FakeClient()
    lines: list[str] = []

    published = publish_messages(
        client,
        build_config(
            count=2,
            topics=(
                build_topic_config(topic="sensors/node-1/temp"),
                build_topic_config(topic="sensors/node-1/humidity", min_value=40.0, max_value=60.0, seed=8),
            ),
        ),
        sleep_fn=lambda _: None,
        emit_line=lines.append,
        now_fn=lambda: datetime(2026, 4, 24, tzinfo=timezone.utc),
    )

    assert published == 4
    assert len(client.publish_calls) == 4
    assert len(lines) == 4


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


def test_run_publishers_handles_multiple_publishers_and_topics() -> None:
    created_clients: list[FakeClient] = []

    def client_factory(*, client_id: str, clean_session: bool):
        client = FakeClient()
        created_clients.append(client)
        return client

    published = run_publishers(
        (
            build_config(
                count=1,
                client_id="publisher-1",
                topics=(
                    build_topic_config(topic="sensors/node-1/temp", trace_id="trace-temp"),
                    build_topic_config(
                        topic="sensors/node-1/humidity",
                        min_value=40.0,
                        max_value=60.0,
                        seed=8,
                        trace_id="trace-humidity",
                    ),
                ),
            ),
            build_config(
                count=1,
                client_id="publisher-2",
                publisher_id="publisher-2",
                topics=(
                    build_topic_config(topic="sensors/node-2/temp", min_value=10.0, max_value=20.0, seed=9),
                ),
            ),
        ),
        client_factory=client_factory,
        sleep_fn=lambda _: None,
        emit_line=lambda _: None,
        now_fn=lambda: datetime(2026, 4, 24, tzinfo=timezone.utc),
        monotonic_fn=lambda: 0.0,
    )

    assert published == 3
    assert len(created_clients) == 2
    assert [call[0] for call in created_clients[0].publish_calls] == [
        "sensors/node-1/temp",
        "sensors/node-1/humidity",
    ]
    assert [call[0] for call in created_clients[1].publish_calls] == ["sensors/node-2/temp"]


def test_config_from_args_builds_valid_config() -> None:
    args = SimpleNamespace(
        config=None,
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
        payload_format="json",
    )

    config = config_from_args(args)

    assert config.count == 5
    assert config.topics[0].seed == 11
    assert config.publisher_id == "publisher-test"
    assert config.topics[0].trace_id == "trace-123"


def test_load_publisher_configs_reads_multiple_publishers(tmp_path: Path) -> None:
    config_path = tmp_path / "publisher.json"
    config_path.write_text(
        json.dumps(
            {
                "publishers": [
                    {
                        "host": "mqtt-broker",
                        "frequency_seconds": 1,
                        "client_id": "pub-1",
                        "topics": [
                            {
                                "topic": "sensors/node-1/temp",
                                "generator": {"kind": "uniform", "min_value": 0, "max_value": 10, "seed": 7},
                            },
                            {
                                "topic": "sensors/node-1/humidity",
                                "generator": {"kind": "uniform", "min_value": 40, "max_value": 60, "seed": 8},
                            },
                        ],
                    },
                    {
                        "host": "mqtt-broker",
                        "frequency_seconds": 2,
                        "client_id": "pub-2",
                        "topics": [
                            {
                                "topic": "sensors/node-2/temp",
                                "generator": {"kind": "uniform", "min_value": 10, "max_value": 20, "seed": 9},
                            }
                        ],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    configs = load_publisher_configs(config_path)

    assert len(configs) == 2
    assert configs[0].client_id == "pub-1"
    assert len(configs[0].topics) == 2
    assert configs[1].topics[0].topic == "sensors/node-2/temp"


def test_load_publisher_configs_reads_clipped_normal_generators(tmp_path: Path) -> None:
    config_path = tmp_path / "publisher.json"
    config_path.write_text(
        json.dumps(
            {
                "publishers": [
                    {
                        "host": "mqtt-broker",
                        "frequency_seconds": 1,
                        "client_id": "pub-1",
                        "topics": [
                            {
                                "topic": "sensors/node-1/temp",
                                "generator": {
                                    "kind": "clipped_normal",
                                    "mean": 12.3,
                                    "stddev": 1.8,
                                    "min_value": 8.0,
                                    "max_value": 16.0,
                                    "seed": 7,
                                },
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    configs = load_publisher_configs(config_path)

    assert configs[0].topics[0].kind == "clipped_normal"
    assert configs[0].topics[0].mean == 12.3
    assert configs[0].topics[0].stddev == 1.8


def test_load_publisher_configs_reads_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "publisher.yaml"
    config_path.write_text(
        """
publishers:
  - host: mqtt-broker
    frequency_seconds: 1
    client_id: pub-1
    topics:
      - topic: sensors/node-1/temp
        generator:
          kind: uniform
          min_value: 0
          max_value: 10
          seed: 7
""".strip(),
        encoding="utf-8",
    )

    configs = load_publisher_configs(config_path)

    assert len(configs) == 1
    assert configs[0].client_id == "pub-1"
    assert configs[0].topics[0].topic == "sensors/node-1/temp"


def test_render_publish_message_contains_core_fields() -> None:
    rendered = render_publish_message(
        index=2,
        topic="sensors/node-1/temp",
        payload="{\"value\":4.2}",
        timestamp=datetime(2026, 4, 24, tzinfo=timezone.utc),
        event_id="event-1",
        trace_id="trace-1",
        client_id="publisher-test",
    )

    assert "index=2" in rendered
    assert "topic=sensors/node-1/temp" in rendered
    assert "client_id=publisher-test" in rendered
    assert "event_id=event-1" in rendered
    assert "trace_id=trace-1" in rendered


def test_main_returns_zero_on_keyboard_interrupt(monkeypatch) -> None:
    monkeypatch.setattr(
        "apps.publisher.cli.run_publisher",
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

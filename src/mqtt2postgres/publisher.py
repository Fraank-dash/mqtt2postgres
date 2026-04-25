from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

import numpy as np
from paho.mqtt import client as mqtt_client

from mqtt2postgres.tracing import build_trace_payload, new_event_id, new_trace_id

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 1883
DEFAULT_CLIENT_ID = "mqtt2postgres-publisher"
DEFAULT_QOS = 0
DEFAULT_PAYLOAD_FORMAT = "json"


class PublisherError(ValueError):
    """Raised when publisher configuration is invalid."""


@dataclass(frozen=True)
class PublisherTopicConfig:
    topic: str
    min_value: float
    max_value: float
    seed: int | None
    trace_id: str | None = None


@dataclass(frozen=True)
class PublisherConfig:
    host: str
    port: int
    frequency_seconds: float
    count: int | None
    client_id: str
    publisher_id: str
    qos: int
    payload_format: str
    topics: tuple[PublisherTopicConfig, ...]


@dataclass
class PublisherTopicState:
    config: PublisherTopicConfig
    rng: np.random.Generator
    trace_id: str
    published_count: int = 0


@dataclass
class PublisherRuntimeState:
    config: PublisherConfig
    client: MQTTClientProtocol
    topic_states: list[PublisherTopicState]
    next_publish_at: float
    cycles_completed: int = 0


class PublishResult(Protocol):
    rc: int


class MQTTClientProtocol(Protocol):
    def connect(self, host: str, port: int) -> None: ...

    def loop_start(self) -> None: ...

    def loop_stop(self) -> None: ...

    def disconnect(self) -> None: ...

    def publish(self, topic: str, payload: str, qos: int = 0) -> PublishResult: ...


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mqtt-publisher",
        description="Publish random numeric MQTT payloads for local testing.",
    )
    parser.add_argument("--config", default=None, help="Path to a JSON publisher config file.")
    parser.add_argument("--host", default=None, help="MQTT broker host.")
    parser.add_argument("--port", type=int, default=None, help="MQTT broker port.")
    parser.add_argument("--topic", default=None, help="MQTT topic to publish to.")
    parser.add_argument(
        "--min-value",
        type=float,
        default=None,
        help="Minimum random value to publish.",
    )
    parser.add_argument(
        "--max-value",
        type=float,
        default=None,
        help="Maximum random value to publish.",
    )
    parser.add_argument(
        "--frequency-seconds",
        type=float,
        default=None,
        help="Publish interval in seconds.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Optional number of publish cycles before exiting.",
    )
    parser.add_argument(
        "--client-id",
        default=None,
        help="MQTT client identifier.",
    )
    parser.add_argument(
        "--publisher-id",
        default=None,
        help="Logical publisher identifier stored in the trace payload. Defaults to the client id.",
    )
    parser.add_argument(
        "--qos",
        type=int,
        default=None,
        choices=(0, 1, 2),
        help="MQTT publish QoS.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional RNG seed for reproducible values.",
    )
    parser.add_argument(
        "--trace-id",
        default=None,
        help="Optional trace id shared by the single CLI topic. Defaults to a generated UUID.",
    )
    parser.add_argument(
        "--payload-format",
        choices=("json", "plain"),
        default=None,
        help="Payload format to publish. Defaults to json traced payloads.",
    )
    return parser


def config_from_args(args: argparse.Namespace) -> PublisherConfig:
    required = {
        "topic": args.topic,
        "min_value": args.min_value,
        "max_value": args.max_value,
        "frequency_seconds": args.frequency_seconds,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        missing_args = ", ".join(f"--{name.replace('_', '-')}" for name in missing)
        raise PublisherError(
            f"Single-topic mode requires these arguments when --config is not used: {missing_args}."
        )

    config = PublisherConfig(
        host=args.host or DEFAULT_HOST,
        port=args.port or DEFAULT_PORT,
        frequency_seconds=args.frequency_seconds,
        count=args.count,
        client_id=args.client_id or DEFAULT_CLIENT_ID,
        publisher_id=args.publisher_id or args.client_id or DEFAULT_CLIENT_ID,
        qos=DEFAULT_QOS if args.qos is None else args.qos,
        payload_format=args.payload_format or DEFAULT_PAYLOAD_FORMAT,
        topics=(
            PublisherTopicConfig(
                topic=args.topic,
                min_value=args.min_value,
                max_value=args.max_value,
                seed=args.seed,
                trace_id=args.trace_id,
            ),
        ),
    )
    validate_config(config)
    return config


def load_publisher_configs(path: str | Path) -> tuple[PublisherConfig, ...]:
    config_path = Path(path)
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PublisherError(f"Publisher config file does not exist: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise PublisherError(f"Publisher config file is not valid JSON: {exc}") from exc

    if not isinstance(raw, dict):
        raise PublisherError("Publisher config root must be a JSON object.")

    publishers_raw = raw.get("publishers")
    if not isinstance(publishers_raw, list) or not publishers_raw:
        raise PublisherError("Publisher config must contain a non-empty 'publishers' array.")

    configs = tuple(parse_publisher_config(item, index=index) for index, item in enumerate(publishers_raw))
    for config in configs:
        validate_config(config)
    return configs


def parse_publisher_config(raw: Any, *, index: int) -> PublisherConfig:
    if not isinstance(raw, dict):
        raise PublisherError(f"Publisher entry #{index + 1} must be a JSON object.")

    host = _optional_text(raw.get("host")) or DEFAULT_HOST
    port = _optional_int(raw.get("port"), field_name=f"publishers[{index}].port", default=DEFAULT_PORT)
    frequency_seconds = _required_float(
        raw.get("frequency_seconds"),
        field_name=f"publishers[{index}].frequency_seconds",
    )
    count = _optional_int(raw.get("count"), field_name=f"publishers[{index}].count", default=None)
    client_id = _optional_text(raw.get("client_id")) or f"{DEFAULT_CLIENT_ID}-{index + 1}"
    publisher_id = _optional_text(raw.get("publisher_id")) or client_id
    qos = _optional_int(raw.get("qos"), field_name=f"publishers[{index}].qos", default=DEFAULT_QOS)
    payload_format = _optional_text(raw.get("payload_format")) or DEFAULT_PAYLOAD_FORMAT

    topics_raw = raw.get("topics")
    if not isinstance(topics_raw, list) or not topics_raw:
        raise PublisherError(f"Publisher entry #{index + 1} must contain a non-empty 'topics' array.")

    topics = tuple(parse_topic_config(item, publisher_index=index, topic_index=topic_index) for topic_index, item in enumerate(topics_raw))
    return PublisherConfig(
        host=host,
        port=port,
        frequency_seconds=frequency_seconds,
        count=count,
        client_id=client_id,
        publisher_id=publisher_id,
        qos=qos,
        payload_format=payload_format,
        topics=topics,
    )


def parse_topic_config(raw: Any, *, publisher_index: int, topic_index: int) -> PublisherTopicConfig:
    if not isinstance(raw, dict):
        raise PublisherError(
            f"Publisher entry #{publisher_index + 1} topic #{topic_index + 1} must be a JSON object."
        )

    topic = _optional_text(raw.get("topic"))
    if not topic:
        raise PublisherError(
            f"Publisher entry #{publisher_index + 1} topic #{topic_index + 1} must define 'topic'."
        )

    generator = raw.get("generator")
    if not isinstance(generator, dict):
        raise PublisherError(
            f"Publisher entry #{publisher_index + 1} topic '{topic}' must define a 'generator' object."
        )

    kind = _optional_text(generator.get("kind")) or "uniform"
    if kind != "uniform":
        raise PublisherError(
            f"Publisher entry #{publisher_index + 1} topic '{topic}' uses unsupported generator kind '{kind}'."
        )

    return PublisherTopicConfig(
        topic=topic,
        min_value=_required_float(
            generator.get("min_value"),
            field_name=f"publishers[{publisher_index}].topics[{topic_index}].generator.min_value",
        ),
        max_value=_required_float(
            generator.get("max_value"),
            field_name=f"publishers[{publisher_index}].topics[{topic_index}].generator.max_value",
        ),
        seed=_optional_int(
            generator.get("seed"),
            field_name=f"publishers[{publisher_index}].topics[{topic_index}].generator.seed",
            default=None,
        ),
        trace_id=_optional_text(raw.get("trace_id")),
    )


def validate_config(config: PublisherConfig) -> None:
    if not config.host.strip():
        raise PublisherError("MQTT host must not be empty.")
    if config.port <= 0:
        raise PublisherError("MQTT port must be greater than zero.")
    if config.frequency_seconds <= 0:
        raise PublisherError("Publish frequency must be greater than zero.")
    if config.count is not None and config.count <= 0:
        raise PublisherError("Publish count must be greater than zero when provided.")
    if config.qos not in (0, 1, 2):
        raise PublisherError("MQTT QoS must be 0, 1, or 2.")
    if config.payload_format not in ("json", "plain"):
        raise PublisherError("Payload format must be 'json' or 'plain'.")
    if not config.topics:
        raise PublisherError("Publisher must define at least one topic.")

    for topic_config in config.topics:
        if not topic_config.topic.strip():
            raise PublisherError("MQTT topic must not be empty.")
        if topic_config.min_value > topic_config.max_value:
            raise PublisherError(
                f"Minimum value must be less than or equal to maximum value for topic '{topic_config.topic}'."
            )


def create_rng(seed: int | None = None) -> np.random.Generator:
    return np.random.default_rng(seed)


def generate_value(config: PublisherTopicConfig, rng: np.random.Generator) -> float:
    return float(rng.uniform(config.min_value, config.max_value))


def format_payload(value: float) -> str:
    return f"{value:.6f}"


def render_publish_message(
    index: int,
    topic: str,
    payload: str,
    timestamp: datetime,
    *,
    event_id: str,
    trace_id: str,
    client_id: str | None = None,
) -> str:
    client_part = f" client_id={client_id}" if client_id else ""
    return (
        f"{timestamp.isoformat()} PUBLISH index={index} topic={topic}"
        f"{client_part} event_id={event_id} trace_id={trace_id} payload={payload}"
    )


def build_topic_state(config: PublisherTopicConfig) -> PublisherTopicState:
    return PublisherTopicState(
        config=config,
        rng=create_rng(config.seed),
        trace_id=config.trace_id or new_trace_id(),
    )


def publish_publisher_cycle(
    client: MQTTClientProtocol,
    config: PublisherConfig,
    topic_states: Sequence[PublisherTopicState],
    *,
    emit_line: Callable[[str], None] = print,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> int:
    published_at = now_fn()
    published = 0

    for topic_state in topic_states:
        value = generate_value(topic_state.config, topic_state.rng)
        sequence = topic_state.published_count + 1
        event_id = new_event_id()
        value_payload = format_payload(value)
        if config.payload_format == "plain":
            payload = value_payload
        else:
            payload = build_trace_payload(
                trace_id=topic_state.trace_id,
                event_id=event_id,
                publisher_id=config.publisher_id,
                sequence=sequence,
                published_at=published_at,
                value=value,
            )
        result = client.publish(topic_state.config.topic, payload, qos=config.qos)
        if getattr(result, "rc", mqtt_client.MQTT_ERR_SUCCESS) != mqtt_client.MQTT_ERR_SUCCESS:
            raise RuntimeError(
                f"Failed to publish MQTT message. Return code: {getattr(result, 'rc', 'unknown')}"
            )

        topic_state.published_count = sequence
        published += 1
        emit_line(
            render_publish_message(
                index=sequence,
                topic=topic_state.config.topic,
                payload=payload,
                timestamp=published_at,
                event_id=event_id,
                trace_id=topic_state.trace_id,
                client_id=config.client_id,
            )
        )

    return published


def publish_messages(
    client: MQTTClientProtocol,
    config: PublisherConfig,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
    emit_line: Callable[[str], None] = print,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> int:
    topic_states = [build_topic_state(topic_config) for topic_config in config.topics]
    published = 0

    while config.count is None or any(state.published_count < config.count for state in topic_states):
        published += publish_publisher_cycle(
            client,
            config,
            topic_states,
            emit_line=emit_line,
            now_fn=now_fn,
        )
        if config.count is not None and all(state.published_count >= config.count for state in topic_states):
            break
        sleep_fn(config.frequency_seconds)

    return published


def run_publisher(
    config: PublisherConfig,
    *,
    client_factory: Callable[..., MQTTClientProtocol] = mqtt_client.Client,
    sleep_fn: Callable[[float], None] = time.sleep,
    emit_line: Callable[[str], None] = print,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> int:
    return run_publishers(
        (config,),
        client_factory=client_factory,
        sleep_fn=sleep_fn,
        emit_line=emit_line,
        now_fn=now_fn,
    )


def run_publishers(
    configs: Sequence[PublisherConfig],
    *,
    client_factory: Callable[..., MQTTClientProtocol] = mqtt_client.Client,
    sleep_fn: Callable[[float], None] = time.sleep,
    emit_line: Callable[[str], None] = print,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    monotonic_fn: Callable[[], float] = time.monotonic,
) -> int:
    if not configs:
        raise PublisherError("At least one publisher configuration is required.")

    states: list[PublisherRuntimeState] = []
    total_published = 0

    for config in configs:
        validate_config(config)
        client = client_factory(client_id=config.client_id, clean_session=True)
        client.connect(config.host, config.port)
        client.loop_start()
        states.append(
            PublisherRuntimeState(
                config=config,
                client=client,
                topic_states=[build_topic_state(topic_config) for topic_config in config.topics],
                next_publish_at=monotonic_fn(),
            )
        )

    try:
        while True:
            active_states = [
                state
                for state in states
                if state.config.count is None or state.cycles_completed < state.config.count
            ]
            if not active_states:
                return total_published

            now_monotonic = monotonic_fn()
            next_due_at: float | None = None
            published_this_round = False

            for state in active_states:
                if state.next_publish_at > now_monotonic:
                    if next_due_at is None or state.next_publish_at < next_due_at:
                        next_due_at = state.next_publish_at
                    continue

                total_published += publish_publisher_cycle(
                    state.client,
                    state.config,
                    state.topic_states,
                    emit_line=emit_line,
                    now_fn=now_fn,
                )
                state.cycles_completed += 1
                state.next_publish_at = monotonic_fn() + state.config.frequency_seconds
                published_this_round = True
                if next_due_at is None or state.next_publish_at < next_due_at:
                    next_due_at = state.next_publish_at

            if published_this_round:
                continue

            if next_due_at is None:
                return total_published
            sleep_fn(max(0.0, next_due_at - now_monotonic))
    finally:
        for state in states:
            state.client.loop_stop()
            state.client.disconnect()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        if args.config:
            configs = load_publisher_configs(args.config)
            run_publishers(configs)
        else:
            config = config_from_args(args)
            run_publisher(config)
    except KeyboardInterrupt:
        print("Publisher stopped.")
        return 0
    except PublisherError as exc:
        parser.error(str(exc))
    except Exception as exc:
        print(f"Publisher failed: {exc}", file=sys.stderr)
        return 1
    return 0


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise PublisherError(f"Expected a string value, got {type(value).__name__}.")
    text = value.strip()
    return text or None


def _required_float(value: Any, *, field_name: str) -> float:
    if value is None:
        raise PublisherError(f"Missing required field '{field_name}'.")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise PublisherError(f"Field '{field_name}' must be numeric.") from exc


def _optional_int(value: Any, *, field_name: str, default: int | None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise PublisherError(f"Field '{field_name}' must be an integer.") from exc


if __name__ == "__main__":
    raise SystemExit(main())

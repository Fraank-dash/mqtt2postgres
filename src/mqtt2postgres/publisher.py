from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol, Sequence

import numpy as np
from paho.mqtt import client as mqtt_client

from mqtt2postgres.tracing import build_trace_payload, new_event_id, new_trace_id


class PublisherError(ValueError):
    """Raised when publisher configuration is invalid."""


@dataclass(frozen=True)
class PublisherConfig:
    host: str
    port: int
    topic: str
    min_value: float
    max_value: float
    frequency_seconds: float
    count: int | None
    client_id: str
    publisher_id: str
    qos: int
    seed: int | None
    trace_id: str | None
    payload_format: str


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
    parser.add_argument("--host", default="127.0.0.1", help="MQTT broker host.")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port.")
    parser.add_argument("--topic", required=True, help="MQTT topic to publish to.")
    parser.add_argument(
        "--min-value",
        type=float,
        required=True,
        help="Minimum random value to publish.",
    )
    parser.add_argument(
        "--max-value",
        type=float,
        required=True,
        help="Maximum random value to publish.",
    )
    parser.add_argument(
        "--frequency-seconds",
        type=float,
        required=True,
        help="Publish interval in seconds.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Optional number of messages to publish before exiting.",
    )
    parser.add_argument(
        "--client-id",
        default="mqtt2postgres-publisher",
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
        default=0,
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
        help="Optional trace id shared by all events in this publisher run. Defaults to a generated UUID.",
    )
    parser.add_argument(
        "--payload-format",
        choices=("json", "plain"),
        default="json",
        help="Payload format to publish. Defaults to json traced payloads.",
    )
    return parser


def config_from_args(args: argparse.Namespace) -> PublisherConfig:
    config = PublisherConfig(
        host=args.host,
        port=args.port,
        topic=args.topic,
        min_value=args.min_value,
        max_value=args.max_value,
        frequency_seconds=args.frequency_seconds,
        count=args.count,
        client_id=args.client_id,
        publisher_id=args.publisher_id or args.client_id,
        qos=args.qos,
        seed=args.seed,
        trace_id=args.trace_id,
        payload_format=args.payload_format,
    )
    validate_config(config)
    return config


def validate_config(config: PublisherConfig) -> None:
    if not config.host.strip():
        raise PublisherError("MQTT host must not be empty.")
    if not config.topic.strip():
        raise PublisherError("MQTT topic must not be empty.")
    if config.port <= 0:
        raise PublisherError("MQTT port must be greater than zero.")
    if config.min_value > config.max_value:
        raise PublisherError("Minimum value must be less than or equal to maximum value.")
    if config.frequency_seconds <= 0:
        raise PublisherError("Publish frequency must be greater than zero.")
    if config.count is not None and config.count <= 0:
        raise PublisherError("Publish count must be greater than zero when provided.")


def create_rng(seed: int | None = None) -> np.random.Generator:
    return np.random.default_rng(seed)


def generate_value(config: PublisherConfig, rng: np.random.Generator) -> float:
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
) -> str:
    return (
        f"{timestamp.isoformat()} PUBLISH index={index} topic={topic} "
        f"event_id={event_id} trace_id={trace_id} payload={payload}"
    )


def publish_messages(
    client: MQTTClientProtocol,
    config: PublisherConfig,
    *,
    rng: np.random.Generator | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    emit_line: Callable[[str], None] = print,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> int:
    rng = rng or create_rng(config.seed)
    trace_id = config.trace_id or new_trace_id()
    published = 0

    while config.count is None or published < config.count:
        value = generate_value(config, rng)
        sequence = published + 1
        published_at = now_fn()
        event_id = new_event_id()
        value_payload = format_payload(value)
        if config.payload_format == "plain":
            payload = value_payload
        else:
            payload = build_trace_payload(
                trace_id=trace_id,
                event_id=event_id,
                publisher_id=config.publisher_id,
                sequence=sequence,
                published_at=published_at,
                value=value,
            )
        result = client.publish(config.topic, payload, qos=config.qos)
        if getattr(result, "rc", mqtt_client.MQTT_ERR_SUCCESS) != mqtt_client.MQTT_ERR_SUCCESS:
            raise RuntimeError(
                f"Failed to publish MQTT message. Return code: {getattr(result, 'rc', 'unknown')}"
            )

        published += 1
        emit_line(
            render_publish_message(
                index=published,
                topic=config.topic,
                payload=payload,
                timestamp=published_at,
                event_id=event_id,
                trace_id=trace_id,
            )
        )
        if config.count is not None and published >= config.count:
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
    client = client_factory(client_id=config.client_id, clean_session=True)
    client.connect(config.host, config.port)
    client.loop_start()
    try:
        return publish_messages(
            client,
            config,
            sleep_fn=sleep_fn,
            emit_line=emit_line,
            now_fn=now_fn,
        )
    finally:
        client.loop_stop()
        client.disconnect()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
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


if __name__ == "__main__":
    raise SystemExit(main())

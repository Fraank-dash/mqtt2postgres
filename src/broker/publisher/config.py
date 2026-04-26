from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from broker.publisher.models import (
    DEFAULT_CLIENT_ID,
    DEFAULT_HOST,
    DEFAULT_PAYLOAD_FORMAT,
    DEFAULT_PORT,
    DEFAULT_QOS,
    SUPPORTED_GENERATOR_KINDS,
    PublisherConfig,
    PublisherError,
    PublisherTopicConfig,
)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mqtt-publisher",
        description="Publish random numeric MQTT payloads for local testing.",
    )
    parser.add_argument("--config", default=None, help="Path to a JSON publisher config file.")
    parser.add_argument("--host", default=None, help="MQTT broker host.")
    parser.add_argument("--port", type=int, default=None, help="MQTT broker port.")
    parser.add_argument("--topic", default=None, help="MQTT topic to publish to.")
    parser.add_argument("--min-value", type=float, default=None, help="Minimum random value to publish.")
    parser.add_argument("--max-value", type=float, default=None, help="Maximum random value to publish.")
    parser.add_argument("--frequency-seconds", type=float, default=None, help="Publish interval in seconds.")
    parser.add_argument("--count", type=int, default=None, help="Optional number of publish cycles before exiting.")
    parser.add_argument("--client-id", default=None, help="MQTT client identifier.")
    parser.add_argument(
        "--publisher-id",
        default=None,
        help="Logical publisher identifier stored in the trace payload. Defaults to the client id.",
    )
    parser.add_argument("--qos", type=int, default=None, choices=(0, 1, 2), help="MQTT publish QoS.")
    parser.add_argument("--seed", type=int, default=None, help="Optional RNG seed for reproducible values.")
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

    topics = tuple(
        parse_topic_config(item, publisher_index=index, topic_index=topic_index)
        for topic_index, item in enumerate(topics_raw)
    )
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
    if kind not in SUPPORTED_GENERATOR_KINDS:
        raise PublisherError(
            f"Publisher entry #{publisher_index + 1} topic '{topic}' uses unsupported generator kind '{kind}'."
        )

    min_value = _required_float(
        generator.get("min_value"),
        field_name=f"publishers[{publisher_index}].topics[{topic_index}].generator.min_value",
    )
    max_value = _required_float(
        generator.get("max_value"),
        field_name=f"publishers[{publisher_index}].topics[{topic_index}].generator.max_value",
    )
    mean: float | None = None
    stddev: float | None = None
    if kind == "clipped_normal":
        mean = _required_float(
            generator.get("mean"),
            field_name=f"publishers[{publisher_index}].topics[{topic_index}].generator.mean",
        )
        stddev = _required_float(
            generator.get("stddev"),
            field_name=f"publishers[{publisher_index}].topics[{topic_index}].generator.stddev",
        )

    return PublisherTopicConfig(
        topic=topic,
        kind=kind,
        min_value=min_value,
        max_value=max_value,
        mean=mean,
        stddev=stddev,
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
        validate_topic_config(topic_config)


def validate_topic_config(config: PublisherTopicConfig) -> None:
    if not config.topic.strip():
        raise PublisherError("MQTT topic must not be empty.")
    if config.kind not in SUPPORTED_GENERATOR_KINDS:
        raise PublisherError(
            f"Generator kind for topic '{config.topic}' must be one of: {', '.join(SUPPORTED_GENERATOR_KINDS)}."
        )
    if config.min_value > config.max_value:
        raise PublisherError(
            f"Minimum value must be less than or equal to maximum value for topic '{config.topic}'."
        )
    if config.kind == "clipped_normal":
        if config.mean is None:
            raise PublisherError(f"Clipped normal generator for topic '{config.topic}' requires mean.")
        if config.stddev is None:
            raise PublisherError(f"Clipped normal generator for topic '{config.topic}' requires stddev.")
        if config.stddev <= 0:
            raise PublisherError(f"Clipped normal generator for topic '{config.topic}' requires stddev > 0.")


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

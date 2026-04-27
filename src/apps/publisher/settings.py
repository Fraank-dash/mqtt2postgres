from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from apps.publisher.models import (
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
    parser.add_argument("--config", default=None, help="Path to a JSON or YAML publisher settings file.")
    parser.add_argument("--host", default=None, help="MQTT broker host.")
    parser.add_argument("--port", type=int, default=None, help="MQTT broker port.")
    parser.add_argument("--mqtt-username", default=None, help="MQTT broker username.")
    parser.add_argument("--mqtt-password", default=None, help="MQTT broker password.")
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
    raw_config = {
        "host": args.host,
        "port": args.port,
        "mqtt_username": args.mqtt_username,
        "mqtt_password": args.mqtt_password,
        "frequency_seconds": args.frequency_seconds,
        "count": args.count,
        "client_id": args.client_id,
        "publisher_id": args.publisher_id,
        "qos": args.qos,
        "payload_format": args.payload_format,
        "topic": args.topic,
        "min_value": args.min_value,
        "max_value": args.max_value,
        "seed": args.seed,
        "trace_id": args.trace_id,
    }
    try:
        return SingleTopicPublisherConfigModel.model_validate(raw_config).to_runtime_config()
    except ValidationError as exc:
        raise _publisher_error_from_validation_error(exc) from exc


def load_publisher_configs(path: str | Path) -> tuple[PublisherConfig, ...]:
    config_path = Path(path)
    raw = load_raw_publisher_settings(config_path)
    try:
        return PublisherConfigDocumentModel.model_validate(raw).to_runtime_configs()
    except ValidationError as exc:
        raise _publisher_error_from_validation_error(exc) from exc


def parse_publisher_config(raw: Any, *, index: int) -> PublisherConfig:
    try:
        return PublisherEntryModel.model_validate(raw, context={"publisher_index": index}).to_runtime_config(index=index)
    except ValidationError as exc:
        raise _publisher_error_from_validation_error(exc) from exc


def parse_topic_config(raw: Any, *, publisher_index: int, topic_index: int) -> PublisherTopicConfig:
    try:
        model = TopicEntryModel.model_validate(
            raw,
            context={"publisher_index": publisher_index, "topic_index": topic_index},
        )
    except ValidationError as exc:
        raise _publisher_error_from_validation_error(exc) from exc
    return model.to_runtime_config()


def validate_config(config: PublisherConfig) -> None:
    try:
        PublisherEntryModel.from_runtime_config(config, index=0)
    except ValidationError as exc:
        raise _publisher_error_from_validation_error(exc) from exc


def validate_topic_config(config: PublisherTopicConfig) -> None:
    try:
        TopicEntryModel.from_runtime_config(config)
    except ValidationError as exc:
        raise _publisher_error_from_validation_error(exc) from exc


def load_raw_publisher_settings(path: str | Path) -> Any:
    config_path = Path(path)
    try:
        raw_text = config_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise PublisherError(f"Publisher settings file does not exist: {config_path}") from exc

    if config_path.suffix.lower() in {".yaml", ".yml"}:
        try:
            return yaml.safe_load(raw_text)
        except yaml.YAMLError as exc:
            raise PublisherError(f"Publisher settings file is not valid YAML: {exc}") from exc

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise PublisherError(f"Publisher settings file is not valid JSON: {exc}") from exc


def _publisher_error_from_validation_error(exc: ValidationError) -> PublisherError:
    error = exc.errors(include_url=False)[0]
    location = ".".join(str(part) for part in error["loc"]) or "settings"
    message = error["msg"]
    return PublisherError(f"{location}: {message}")


def _strip_text(value: str, field_name: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError(f"{field_name} must not be empty.")
    return text


class PublisherGeneratorModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = "uniform"
    min_value: float
    max_value: float
    seed: int | None = None
    mean: float | None = None
    stddev: float | None = None

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: str) -> str:
        normalized = _strip_text(value, "generator.kind")
        if normalized not in SUPPORTED_GENERATOR_KINDS:
            raise ValueError(f"must be one of: {', '.join(SUPPORTED_GENERATOR_KINDS)}")
        return normalized

    @model_validator(mode="after")
    def validate_generator(self) -> "PublisherGeneratorModel":
        if self.min_value > self.max_value:
            raise ValueError("min_value must be less than or equal to max_value.")
        if self.kind == "clipped_normal":
            if self.mean is None:
                raise ValueError("clipped_normal requires mean.")
            if self.stddev is None:
                raise ValueError("clipped_normal requires stddev.")
            if self.stddev <= 0:
                raise ValueError("clipped_normal requires stddev > 0.")
        return self


class TopicEntryModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str
    trace_id: str | None = None
    generator: PublisherGeneratorModel

    @field_validator("topic")
    @classmethod
    def validate_topic(cls, value: str) -> str:
        return _strip_text(value, "topic")

    @field_validator("trace_id")
    @classmethod
    def validate_trace_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _strip_text(value, "trace_id")

    def to_runtime_config(self) -> PublisherTopicConfig:
        return PublisherTopicConfig(
            topic=self.topic,
            min_value=self.generator.min_value,
            max_value=self.generator.max_value,
            seed=self.generator.seed,
            trace_id=self.trace_id,
            kind=self.generator.kind,
            mean=self.generator.mean,
            stddev=self.generator.stddev,
        )

    @classmethod
    def from_runtime_config(cls, config: PublisherTopicConfig) -> "TopicEntryModel":
        return cls(
            topic=config.topic,
            trace_id=config.trace_id,
            generator=PublisherGeneratorModel(
                kind=config.kind,
                min_value=config.min_value,
                max_value=config.max_value,
                seed=config.seed,
                mean=config.mean,
                stddev=config.stddev,
            ),
        )


class PublisherEntryModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    frequency_seconds: float
    count: int | None = None
    client_id: str | None = None
    publisher_id: str | None = None
    qos: int = DEFAULT_QOS
    payload_format: str = DEFAULT_PAYLOAD_FORMAT
    topics: list[TopicEntryModel]

    @field_validator("host")
    @classmethod
    def validate_host(cls, value: str) -> str:
        return _strip_text(value, "host")

    @field_validator("port")
    @classmethod
    def validate_port(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be greater than zero.")
        return value

    @field_validator("frequency_seconds")
    @classmethod
    def validate_frequency_seconds(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("must be greater than zero.")
        return value

    @field_validator("count")
    @classmethod
    def validate_count(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("must be greater than zero when provided.")
        return value

    @field_validator("client_id")
    @classmethod
    def validate_client_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _strip_text(value, "client_id")

    @field_validator("mqtt_username")
    @classmethod
    def validate_mqtt_username(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _strip_text(value, "mqtt_username")

    @field_validator("mqtt_password")
    @classmethod
    def validate_mqtt_password(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _strip_text(value, "mqtt_password")

    @field_validator("publisher_id")
    @classmethod
    def validate_publisher_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _strip_text(value, "publisher_id")

    @field_validator("qos")
    @classmethod
    def validate_qos(cls, value: int) -> int:
        if value not in (0, 1, 2):
            raise ValueError("must be 0, 1, or 2.")
        return value

    @field_validator("payload_format")
    @classmethod
    def validate_payload_format(cls, value: str) -> str:
        normalized = _strip_text(value, "payload_format")
        if normalized not in ("json", "plain"):
            raise ValueError("must be 'json' or 'plain'.")
        return normalized

    @field_validator("topics")
    @classmethod
    def validate_topics(cls, value: list[TopicEntryModel]) -> list[TopicEntryModel]:
        if not value:
            raise ValueError("must contain at least one topic.")
        return value

    @model_validator(mode="after")
    def validate_mqtt_credentials(self) -> "PublisherEntryModel":
        if self.mqtt_username and not self.mqtt_password:
            raise ValueError("mqtt_password is required when mqtt_username is configured.")
        if self.mqtt_password and not self.mqtt_username:
            raise ValueError("mqtt_username is required when mqtt_password is configured.")
        return self

    def to_runtime_config(self, *, index: int) -> PublisherConfig:
        client_id = self.client_id or f"{DEFAULT_CLIENT_ID}-{index + 1}"
        publisher_id = self.publisher_id or client_id
        return PublisherConfig(
            host=self.host,
            port=self.port,
            mqtt_username=self.mqtt_username,
            mqtt_password=self.mqtt_password,
            frequency_seconds=self.frequency_seconds,
            count=self.count,
            client_id=client_id,
            publisher_id=publisher_id,
            qos=self.qos,
            payload_format=self.payload_format,
            topics=tuple(topic.to_runtime_config() for topic in self.topics),
        )

    @classmethod
    def from_runtime_config(cls, config: PublisherConfig, *, index: int) -> "PublisherEntryModel":
        default_client_id = f"{DEFAULT_CLIENT_ID}-{index + 1}"
        publisher_id = config.publisher_id if config.publisher_id != config.client_id else None
        return cls(
            host=config.host,
            port=config.port,
            mqtt_username=config.mqtt_username,
            mqtt_password=config.mqtt_password,
            frequency_seconds=config.frequency_seconds,
            count=config.count,
            client_id=None if config.client_id == default_client_id else config.client_id,
            publisher_id=publisher_id,
            qos=config.qos,
            payload_format=config.payload_format,
            topics=[TopicEntryModel.from_runtime_config(topic) for topic in config.topics],
        )


class PublisherConfigDocumentModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    publishers: list[PublisherEntryModel]

    @field_validator("publishers")
    @classmethod
    def validate_publishers(cls, value: list[PublisherEntryModel]) -> list[PublisherEntryModel]:
        if not value:
            raise ValueError("must contain at least one publisher.")
        return value

    def to_runtime_configs(self) -> tuple[PublisherConfig, ...]:
        return tuple(
            publisher.to_runtime_config(index=index)
            for index, publisher in enumerate(self.publishers)
        )


class SingleTopicPublisherConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str | None = None
    port: int | None = None
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    frequency_seconds: float | None = None
    count: int | None = None
    client_id: str | None = None
    publisher_id: str | None = None
    qos: int | None = None
    payload_format: str | None = None
    topic: str | None = None
    min_value: float | None = Field(default=None)
    max_value: float | None = Field(default=None)
    seed: int | None = None
    trace_id: str | None = None

    @model_validator(mode="after")
    def validate_required_cli_fields(self) -> "SingleTopicPublisherConfigModel":
        required = {
            "topic": self.topic,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "frequency_seconds": self.frequency_seconds,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            missing_args = ", ".join(f"--{name.replace('_', '-')}" for name in missing)
            raise ValueError(
                f"Single-topic mode requires these arguments when --config is not used: {missing_args}."
            )
        if self.mqtt_username and not self.mqtt_password:
            raise ValueError("--mqtt-password is required when --mqtt-username is configured.")
        if self.mqtt_password and not self.mqtt_username:
            raise ValueError("--mqtt-username is required when --mqtt-password is configured.")
        return self

    def to_runtime_config(self) -> PublisherConfig:
        assert self.topic is not None
        assert self.min_value is not None
        assert self.max_value is not None
        assert self.frequency_seconds is not None
        entry = PublisherEntryModel(
            host=self.host or DEFAULT_HOST,
            port=self.port or DEFAULT_PORT,
            mqtt_username=self.mqtt_username,
            mqtt_password=self.mqtt_password,
            frequency_seconds=self.frequency_seconds,
            count=self.count,
            client_id=self.client_id or DEFAULT_CLIENT_ID,
            publisher_id=self.publisher_id or self.client_id or DEFAULT_CLIENT_ID,
            qos=DEFAULT_QOS if self.qos is None else self.qos,
            payload_format=self.payload_format or DEFAULT_PAYLOAD_FORMAT,
            topics=[
                TopicEntryModel(
                    topic=self.topic,
                    trace_id=self.trace_id,
                    generator=PublisherGeneratorModel(
                        min_value=self.min_value,
                        max_value=self.max_value,
                        seed=self.seed,
                    ),
                )
            ],
        )
        return entry.to_runtime_config(index=0)

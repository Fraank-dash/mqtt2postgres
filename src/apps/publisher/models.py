from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import numpy as np

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 1883
DEFAULT_CLIENT_ID = "mqtt2postgres-publisher"
DEFAULT_QOS = 0
DEFAULT_PAYLOAD_FORMAT = "json"
SUPPORTED_GENERATOR_KINDS = ("uniform", "clipped_normal")


class PublisherError(ValueError):
    """Raised when publisher configuration is invalid."""


@dataclass(frozen=True)
class PublisherTopicConfig:
    topic: str
    min_value: float
    max_value: float
    seed: int | None
    trace_id: str | None = None
    kind: str = "uniform"
    mean: float | None = None
    stddev: float | None = None


@dataclass(frozen=True)
class PublisherConfig:
    host: str
    port: int
    mqtt_username: str | None
    mqtt_password: str | None
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
    client: "MQTTClientProtocol"
    topic_states: list[PublisherTopicState]
    next_publish_at: float
    cycles_completed: int = 0


class PublishResult(Protocol):
    rc: int


class MQTTClientProtocol(Protocol):
    def username_pw_set(self, username: str, password: str | None = None) -> None: ...

    def connect(self, host: str, port: int) -> None: ...

    def loop_start(self) -> None: ...

    def loop_stop(self) -> None: ...

    def disconnect(self) -> None: ...

    def publish(self, topic: str, payload: str, qos: int = 0) -> PublishResult: ...

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from datetime import datetime, timezone

import numpy as np
from paho.mqtt import client as mqtt_client

from broker.publisher.config import validate_config
from broker.publisher.models import (
    MQTTClientProtocol,
    PublisherConfig,
    PublisherError,
    PublisherRuntimeState,
    PublisherTopicConfig,
    PublisherTopicState,
)
from observability.tracing import build_trace_payload, new_event_id, new_trace_id


def create_rng(seed: int | None = None) -> np.random.Generator:
    return np.random.default_rng(seed)


def generate_value(config: PublisherTopicConfig, rng: np.random.Generator) -> float:
    if config.kind == "uniform":
        return float(rng.uniform(config.min_value, config.max_value))
    if config.kind == "clipped_normal":
        assert config.mean is not None
        assert config.stddev is not None
        return float(np.clip(rng.normal(config.mean, config.stddev), config.min_value, config.max_value))
    raise PublisherError(f"Unsupported generator kind '{config.kind}' for topic '{config.topic}'.")


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

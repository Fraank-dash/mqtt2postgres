from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Protocol

from paho.mqtt import client as mqtt_client

from broker.client import create_subscriber_client, topic_matches
from mqtt2postgres.config import AppConfig
from mqtt2postgres.db import DatabaseFunctionWriter
from observability.tracing import TraceEnvelope, parse_trace_payload


class EventEmitter(Protocol):
    def emit(self, event: str, **kwargs) -> None: ...


@dataclass
class MQTTToPostgresService:
    config: AppConfig
    event_logger: EventEmitter
    writer_factory: Callable[..., DatabaseFunctionWriter] = DatabaseFunctionWriter.from_config
    mqtt_client_factory: Callable[..., mqtt_client.Client] = create_subscriber_client
    writer: DatabaseFunctionWriter = field(init=False)
    has_started: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self.writer = self.writer_factory(config=self.config)
        self.client = self.mqtt_client_factory(
            config=self.config,
            on_connect=self.on_connect,
            on_message=self.on_message,
            on_disconnect=self.on_disconnect,
        )

    def start(self) -> None:
        self.event_logger.emit(
            "mqtt.connecting",
            component="mqtt",
            message="Connecting to MQTT broker.",
            status="connecting",
            details={
                "host": self.config.mqtt_host,
                "port": self.config.mqtt_port,
            },
        )
        self.client.connect(
            host=self.config.mqtt_host,
            port=self.config.mqtt_port,
        )
        try:
            self.client.loop_forever()
        finally:
            self.writer.close()

    def matched_topic_filter(self, topic: str) -> str | None:
        for topic_filter in self.config.topic_filters:
            if topic_matches(topic_filter, topic):
                return topic_filter
        return None

    def on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if rc != 0:
            self.event_logger.emit(
                "mqtt.connect_failed",
                component="mqtt",
                message="Failed to connect to MQTT broker.",
                level="ERROR",
                status="failed",
                details={"return_code": rc},
            )
            raise RuntimeError(f"Failed to connect to MQTT broker. Return code: {rc}")

        self.event_logger.emit(
            "mqtt.connected",
            component="mqtt",
            message="Connected to MQTT broker.",
            details={"return_code": rc},
        )

        seen_topics: set[str] = set()
        for topic_filter in self.config.topic_filters:
            if topic_filter in seen_topics:
                continue
            seen_topics.add(topic_filter)
            client.subscribe(topic_filter, qos=self.config.mqtt_qos)
            self.event_logger.emit(
                "mqtt.subscribed",
                component="mqtt",
                message="Subscribed to MQTT topic filter.",
                topic=topic_filter,
                details={"qos": self.config.mqtt_qos},
            )

        if not self.has_started:
            self.has_started = True
            self.event_logger.emit(
                "service.started",
                component="service",
                message="MQTT to Postgres service started.",
                details={"topic_filter_count": len(self.config.topic_filters)},
            )

    def on_message(self, client, userdata, message: mqtt_client.MQTTMessage) -> None:
        payload = message.payload.decode("utf-8", errors="replace")
        trace = parse_trace_payload(payload)
        self.event_logger.emit(
            "message.received",
            component="service",
            message="Received MQTT message.",
            topic=message.topic,
            status="received",
            details={
                "payload_size": len(message.payload),
                "event_id": trace.event_id,
                "trace_id": trace.trace_id,
                "sequence": trace.sequence,
                "published_at": trace.published_at.isoformat() if trace.published_at else None,
                "publisher_id": trace.publisher_id,
            },
        )

        topic_filter = self.matched_topic_filter(message.topic)
        if topic_filter is None:
            self.event_logger.emit(
                "message.unrouted",
                component="service",
                message="No configured topic filter matched the MQTT topic.",
                level="WARNING",
                topic=message.topic,
                status="unrouted",
            )
            return

        self.event_logger.emit(
            "message.routed",
            component="service",
            message="Matched MQTT message to a subscribed topic filter.",
            topic=message.topic,
            status="routed",
            details={
                "event_id": trace.event_id,
                "trace_id": trace.trace_id,
                "sequence": trace.sequence,
                "topic_filter": topic_filter,
            },
        )
        received_at = datetime.now(timezone.utc)
        self.event_logger.emit(
            "db.insert_attempted",
            component="db",
            message="Attempting to pass MQTT event to Postgres ingest function.",
            topic=message.topic,
            status="inserting",
            details={
                "event_id": trace.event_id,
                "trace_id": trace.trace_id,
                "sequence": trace.sequence,
                "received_at": received_at.isoformat(),
            },
        )
        try:
            write_result = self.writer.insert_message(
                topic=message.topic,
                payload=payload,
                metadata=build_message_metadata(message, trace=trace, topic_filter=topic_filter),
                received_at=received_at,
            )
        except Exception as exc:
            self.event_logger.emit(
                "db.write_failed",
                component="db",
                message="Failed to write MQTT message to Postgres.",
                level="ERROR",
                topic=message.topic,
                status="failed",
                details={
                    "event_id": trace.event_id,
                    "trace_id": trace.trace_id,
                    "sequence": trace.sequence,
                    "error_class": type(exc).__name__,
                    "error_message": str(exc),
                },
            )
            return

        self.event_logger.emit(
            "db.write_succeeded",
            component="db",
            message="Wrote MQTT message to Postgres.",
            topic=message.topic,
            status="written",
            details={
                "payload_size": len(message.payload),
                "event_id": trace.event_id,
                "trace_id": trace.trace_id,
                "sequence": trace.sequence,
                "published_at": trace.published_at.isoformat() if trace.published_at else None,
                "received_at": received_at.isoformat(),
                "committed_at": write_result["committed_at"].isoformat(),
            },
        )

    def on_disconnect(self, client, userdata, rc, properties=None, reason_code=None) -> None:
        level = "INFO" if rc == 0 else "WARNING"
        status = "disconnected" if rc == 0 else "unexpected_disconnect"
        self.event_logger.emit(
            "mqtt.disconnected",
            component="mqtt",
            message="Disconnected from MQTT broker.",
            level=level,
            status=status,
            details={"return_code": rc},
        )


def build_message_metadata(
    message: mqtt_client.MQTTMessage,
    *,
    trace: TraceEnvelope,
    topic_filter: str,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "topic_filter": topic_filter,
        "payload_size": len(message.payload),
        "mqtt_qos": getattr(message, "qos", None),
        "mqtt_retain": getattr(message, "retain", None),
        "mqtt_mid": getattr(message, "mid", None),
        "event_id": trace.event_id,
        "trace_id": trace.trace_id,
        "publisher_id": trace.publisher_id,
        "sequence": trace.sequence,
        "published_at": trace.published_at.isoformat() if trace.published_at else None,
    }
    return {key: value for key, value in metadata.items() if value is not None}

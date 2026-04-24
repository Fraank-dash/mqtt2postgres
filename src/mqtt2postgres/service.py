from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Protocol

from paho.mqtt import client as mqtt_client

from mqtt2postgres.config import AppConfig, Route
from mqtt2postgres.db import DatabaseWriter
from mqtt2postgres.mqtt import create_mqtt_client
from mqtt2postgres.tracing import parse_trace_payload


class EventEmitter(Protocol):
    def emit(self, event: str, **kwargs) -> None: ...


@dataclass
class MQTTToPostgresService:
    config: AppConfig
    event_logger: EventEmitter
    writer_factory: Callable[..., DatabaseWriter] = DatabaseWriter.from_route
    mqtt_client_factory: Callable[..., mqtt_client.Client] = create_mqtt_client
    writers: dict[str, DatabaseWriter] = field(init=False)
    has_started: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self.writers = {}
        for route in self.config.routes:
            if route.table_name in self.writers:
                continue
            self.writers[route.table_name] = self.writer_factory(
                route=route,
                config=self.config,
            )
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
            for writer in self.writers.values():
                writer.close()

    def resolve_route(self, topic: str) -> Route | None:
        for route in self.config.routes:
            if topic_matches(route.topic_filter, topic):
                return route
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
        for route in self.config.routes:
            if route.topic_filter in seen_topics:
                continue
            seen_topics.add(route.topic_filter)
            client.subscribe(route.topic_filter, qos=self.config.mqtt_qos)
            self.event_logger.emit(
                "mqtt.subscribed",
                component="mqtt",
                message="Subscribed to MQTT topic filter.",
                topic=route.topic_filter,
                details={"qos": self.config.mqtt_qos},
            )

        if not self.has_started:
            self.has_started = True
            self.event_logger.emit(
                "service.started",
                component="service",
                message="MQTT to Postgres service started.",
                details={"route_count": len(self.config.routes)},
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

        route = self.resolve_route(message.topic)
        if route is None:
            self.event_logger.emit(
                "message.unrouted",
                component="service",
                message="No route matched the MQTT topic.",
                level="WARNING",
                topic=message.topic,
                status="unrouted",
            )
            return

        writer = self.writers[route.table_name]
        self.event_logger.emit(
            "message.routed",
            component="service",
            message="Routed MQTT message to a table.",
            topic=message.topic,
            table=route.table_name,
            status="routed",
            details={
                "event_id": trace.event_id,
                "trace_id": trace.trace_id,
                "sequence": trace.sequence,
                "topic_filter": route.topic_filter,
            },
        )
        received_at = datetime.now(timezone.utc)
        self.event_logger.emit(
            "db.insert_attempted",
            component="db",
            message="Attempting to insert traced MQTT event into Postgres.",
            topic=message.topic,
            table=route.table_name,
            status="inserting",
            details={
                "event_id": trace.event_id,
                "trace_id": trace.trace_id,
                "sequence": trace.sequence,
                "received_at": received_at.isoformat(),
            },
        )
        try:
            write_result = writer.insert_message(
                topic=message.topic,
                payload=trace.value,
                trace=trace,
                received_at=received_at,
            )
        except Exception as exc:
            self.event_logger.emit(
                "db.write_failed",
                component="db",
                message="Failed to write MQTT message to Postgres.",
                level="ERROR",
                topic=message.topic,
                table=route.table_name,
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
            table=route.table_name,
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


def topic_matches(topic_pattern: str, topic: str) -> bool:
    return mqtt_client.topic_matches_sub(topic_pattern, topic)

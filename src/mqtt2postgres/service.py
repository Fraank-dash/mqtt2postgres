from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Protocol

from paho.mqtt import client as mqtt_client

from mqtt2postgres.config import AppConfig
from mqtt2postgres.contracts import DerivedContract
from mqtt2postgres.db import DatabaseWriter
from mqtt2postgres.mqtt import create_mqtt_client


class EventEmitter(Protocol):
    def emit(self, event: str, **kwargs) -> None: ...


@dataclass
class MQTTToPostgresService:
    config: AppConfig
    event_logger: EventEmitter
    writer_factory: Callable[..., DatabaseWriter] = DatabaseWriter.from_contract
    mqtt_client_factory: Callable[..., mqtt_client.Client] = create_mqtt_client
    writers: dict[str, DatabaseWriter] = field(init=False)
    has_started: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self.writers = {}
        for contract in self.config.derived_contracts:
            contract_key = str(contract.path)
            if contract_key in self.writers:
                continue
            self.writers[contract_key] = self.writer_factory(
                contract=contract,
                username=self.config.db_username,
                password=self.config.db_password,
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
            broker_contract=self.config.broker_contract,
            status="connecting",
            details={
                "host": self.config.broker_contract.server.host,
                "port": self.config.broker_contract.server.port,
            },
        )
        self.client.connect(
            host=self.config.broker_contract.server.host,
            port=self.config.broker_contract.server.port,
        )
        try:
            self.client.loop_forever()
        finally:
            for writer in self.writers.values():
                writer.close()

    def resolve_contract(self, topic: str) -> DerivedContract | None:
        for contract in self.config.derived_contracts:
            for topic_filter in contract.source_topic_filters:
                if topic_matches(topic_filter, topic):
                    return contract
        return None

    def on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if rc != 0:
            self.event_logger.emit(
                "mqtt.connect_failed",
                component="mqtt",
                message="Failed to connect to MQTT broker.",
                level="ERROR",
                broker_contract=self.config.broker_contract,
                status="failed",
                details={"return_code": rc},
            )
            raise RuntimeError(f"Failed to connect to MQTT broker. Return code: {rc}")

        self.event_logger.emit(
            "mqtt.connected",
            component="mqtt",
            message="Connected to MQTT broker.",
            broker_contract=self.config.broker_contract,
            details={"return_code": rc},
        )

        seen_topics: set[str] = set()
        qos = self.config.broker_contract.server.qos
        for topic_filter in self.config.broker_contract.server.topic_filters:
            if topic_filter in seen_topics:
                continue
            seen_topics.add(topic_filter)
            client.subscribe(topic_filter, qos=qos)
            self.event_logger.emit(
                "mqtt.subscribed",
                component="mqtt",
                message="Subscribed to MQTT topic filter.",
                broker_contract=self.config.broker_contract,
                topic=topic_filter,
                details={"qos": qos},
            )

        if not self.has_started:
            self.has_started = True
            self.event_logger.emit(
                "service.started",
                component="service",
                message="MQTT to Postgres service started.",
                broker_contract=self.config.broker_contract,
                details={"derived_contract_count": len(self.config.derived_contracts)},
            )

    def on_message(self, client, userdata, message: mqtt_client.MQTTMessage) -> None:
        self.event_logger.emit(
            "message.received",
            component="service",
            message="Received MQTT message.",
            broker_contract=self.config.broker_contract,
            topic=message.topic,
            status="received",
            details={"payload_size": len(message.payload)},
        )

        contract = self.resolve_contract(message.topic)
        if contract is None:
            self.event_logger.emit(
                "message.unrouted",
                component="service",
                message="No derived contract matched the MQTT topic.",
                level="WARNING",
                broker_contract=self.config.broker_contract,
                topic=message.topic,
                status="unrouted",
            )
            return

        payload = message.payload.decode("utf-8", errors="replace")
        writer = self.writers[str(contract.path)]
        self.event_logger.emit(
            "message.routed",
            component="service",
            message="Routed MQTT message to a derived contract.",
            broker_contract=self.config.broker_contract,
            derived_contract=contract,
            topic=message.topic,
            table=contract.table_name,
            status="routed",
        )
        message_time = datetime.now(timezone.utc)
        try:
            writer.insert_message(
                topic=message.topic,
                payload=payload,
                message_time=message_time,
            )
        except Exception as exc:
            self.event_logger.emit(
                "db.write_failed",
                component="db",
                message="Failed to write MQTT message to Postgres.",
                level="ERROR",
                broker_contract=self.config.broker_contract,
                derived_contract=contract,
                topic=message.topic,
                table=contract.table_name,
                status="failed",
                details={
                    "error_class": type(exc).__name__,
                    "error_message": str(exc),
                },
            )
            return

        self.event_logger.emit(
            "db.write_succeeded",
            component="db",
            message="Wrote MQTT message to Postgres.",
            broker_contract=self.config.broker_contract,
            derived_contract=contract,
            topic=message.topic,
            table=contract.table_name,
            status="written",
            details={"payload_size": len(message.payload), "message_time": message_time.isoformat()},
        )

    def on_disconnect(self, client, userdata, rc, properties=None, reason_code=None) -> None:
        level = "INFO" if rc == 0 else "WARNING"
        status = "disconnected" if rc == 0 else "unexpected_disconnect"
        self.event_logger.emit(
            "mqtt.disconnected",
            component="mqtt",
            message="Disconnected from MQTT broker.",
            level=level,
            broker_contract=self.config.broker_contract,
            status=status,
            details={"return_code": rc},
        )


def topic_matches(topic_pattern: str, topic: str) -> bool:
    return mqtt_client.topic_matches_sub(topic_pattern, topic)

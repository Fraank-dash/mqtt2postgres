from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from paho.mqtt import client as mqtt_client

from mqtt2postgres.config import AppConfig, TopicMapping
from mqtt2postgres.db import DatabaseWriter
from mqtt2postgres.mqtt import create_mqtt_client


@dataclass
class MQTTToPostgresService:
    config: AppConfig
    writer: DatabaseWriter = field(init=False)

    def __post_init__(self) -> None:
        self.writer = DatabaseWriter.from_config(self.config)
        self.client = create_mqtt_client(
            config=self.config,
            on_connect=self.on_connect,
            on_message=self.on_message,
        )

    def start(self) -> None:
        self.client.connect(host=self.config.mqtt_host, port=self.config.mqtt_port)
        try:
            self.client.loop_forever()
        finally:
            self.writer.close()

    def resolve_table(self, topic: str) -> str | None:
        for mapping in self.config.mappings:
            if topic_matches(mapping, topic):
                return mapping.table_name
        return None

    def on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if rc != 0:
            raise RuntimeError(f"Failed to connect to MQTT broker. Return code: {rc}")

        seen_topics: set[str] = set()
        for mapping in self.config.mappings:
            if mapping.topic_pattern in seen_topics:
                continue
            seen_topics.add(mapping.topic_pattern)
            client.subscribe(mapping.topic_pattern, qos=self.config.qos)

    def on_message(self, client, userdata, message: mqtt_client.MQTTMessage) -> None:
        table_name = self.resolve_table(message.topic)
        if table_name is None:
            return

        payload = message.payload.decode("utf-8", errors="replace")
        self.writer.insert_message(
            table_name=table_name,
            topic=message.topic,
            payload=payload,
            message_time=datetime.now(timezone.utc),
        )


def topic_matches(mapping: TopicMapping, topic: str) -> bool:
    return mqtt_client.topic_matches_sub(mapping.topic_pattern, topic)

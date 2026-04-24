from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from paho.mqtt import client as mqtt_client

from mqtt2postgres.config import AppConfig
from mqtt2postgres.db import DatabaseWriter
from mqtt2postgres.mqtt import create_mqtt_client


@dataclass
class MQTTToPostgresService:
    config: AppConfig
    writers: dict[str, DatabaseWriter] = field(init=False)

    def __post_init__(self) -> None:
        self.writers = {}
        for contract in self.config.derived_contracts:
            contract_key = str(contract.path)
            if contract_key in self.writers:
                continue
            self.writers[contract_key] = DatabaseWriter.from_contract(
                contract=contract,
                username=self.config.db_username,
                password=self.config.db_password,
            )
        self.client = create_mqtt_client(
            config=self.config,
            on_connect=self.on_connect,
            on_message=self.on_message,
        )

    def start(self) -> None:
        self.client.connect(
            host=self.config.broker_contract.server.host,
            port=self.config.broker_contract.server.port,
        )
        try:
            self.client.loop_forever()
        finally:
            for writer in self.writers.values():
                writer.close()

    def resolve_contract(self, topic: str):
        for contract in self.config.derived_contracts:
            for topic_filter in contract.source_topic_filters:
                if topic_matches(topic_filter, topic):
                    return contract
        return None

    def on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if rc != 0:
            raise RuntimeError(f"Failed to connect to MQTT broker. Return code: {rc}")

        seen_topics: set[str] = set()
        qos = self.config.broker_contract.server.qos
        for topic_filter in self.config.broker_contract.server.topic_filters:
            if topic_filter in seen_topics:
                continue
            seen_topics.add(topic_filter)
            client.subscribe(topic_filter, qos=qos)

    def on_message(self, client, userdata, message: mqtt_client.MQTTMessage) -> None:
        contract = self.resolve_contract(message.topic)
        if contract is None:
            return

        payload = message.payload.decode("utf-8", errors="replace")
        writer = self.writers[str(contract.path)]
        writer.insert_message(
            topic=message.topic,
            payload=payload,
            message_time=datetime.now(timezone.utc),
        )


def topic_matches(topic_pattern: str, topic: str) -> bool:
    return mqtt_client.topic_matches_sub(topic_pattern, topic)

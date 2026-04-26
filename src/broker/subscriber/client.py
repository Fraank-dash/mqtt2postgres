from __future__ import annotations

from collections.abc import Callable

from paho.mqtt import client as mqtt_client

from apps.subscriber.models import SubscriberSettings


def create_subscriber_client(
    config: SubscriberSettings,
    on_connect: Callable[..., None],
    on_message: Callable[..., None],
    on_disconnect: Callable[..., None],
) -> mqtt_client.Client:
    client = mqtt_client.Client(client_id=config.mqtt_client_id, clean_session=True)
    if config.mqtt_username:
        client.username_pw_set(
            username=config.mqtt_username,
            password=config.mqtt_password,
        )
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    return client


def topic_matches(topic_pattern: str, topic: str) -> bool:
    return mqtt_client.topic_matches_sub(topic_pattern, topic)

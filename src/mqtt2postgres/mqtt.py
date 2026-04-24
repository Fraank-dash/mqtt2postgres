from __future__ import annotations

from paho.mqtt import client as mqtt_client

from mqtt2postgres.config import AppConfig


def create_mqtt_client(config: AppConfig, on_connect, on_message, on_disconnect) -> mqtt_client.Client:
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

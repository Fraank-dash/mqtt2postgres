from mqtt2postgres.config import AppConfig, TopicMapping
from mqtt2postgres.service import topic_matches


def build_config() -> AppConfig:
    return AppConfig(
        db_host="db",
        db_port=5432,
        db_name="mqtt",
        db_schema="public",
        db_username="postgres",
        db_password="secret",
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_username=None,
        mqtt_password=None,
        mqtt_client_id="mqtt2postgres",
        qos=0,
        mappings=(
            TopicMapping(topic_pattern="devices/+/temp", table_name="tbl_temp"),
            TopicMapping(topic_pattern="$SYS/broker/#", table_name="tbl_sys"),
        ),
    )


def test_topic_matches_single_level_wildcard() -> None:
    config = build_config()

    assert topic_matches(config.mappings[0], "devices/node-1/temp") is True
    assert topic_matches(config.mappings[0], "devices/node-1/humidity") is False


def test_topic_matches_multi_level_wildcard() -> None:
    config = build_config()

    assert topic_matches(config.mappings[1], "$SYS/broker/clients/total") is True
    assert topic_matches(config.mappings[1], "$SYS/other") is False

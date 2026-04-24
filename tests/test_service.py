from datetime import datetime, timezone
from types import SimpleNamespace

from mqtt2postgres.config import AppConfig, Route
from mqtt2postgres.service import MQTTToPostgresService, topic_matches
from mqtt2postgres.tracing import build_trace_payload


def build_config() -> AppConfig:
    return AppConfig(
        mqtt_host="localhost",
        mqtt_port=1883,
        mqtt_username=None,
        mqtt_password=None,
        mqtt_client_id="mqtt2postgres",
        mqtt_qos=0,
        db_host="localhost",
        db_port=5432,
        db_name="mqtt",
        db_schema="public",
        db_username="postgres",
        db_password="secret",
        routes=(
            Route(topic_filter="devices/+/temp", table_name="tbl_temp"),
            Route(topic_filter="$SYS/broker/#", table_name="tbl_sys"),
        ),
        log_format="json",
        log_level="INFO",
    )


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def emit(self, event: str, **kwargs) -> None:
        self.events.append((event, kwargs))


class FakeWriter:
    def __init__(self, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.closed = False
        self.calls: list[dict] = []

    def insert_message(self, *, topic: str, payload: str, trace=None, received_at=None):
        self.calls.append(
            {
                "topic": topic,
                "payload": payload,
                "trace": trace,
                "received_at": received_at,
            }
        )
        if self.should_fail:
            raise RuntimeError("db write failed")
        return {"result": "ok", "committed_at": received_at}

    def close(self) -> None:
        self.closed = True


class FakeClient:
    def __init__(self) -> None:
        self.subscriptions: list[tuple[str, int]] = []
        self.connect_calls: list[tuple[str, int]] = []
        self.loop_forever_called = False

    def connect(self, host: str, port: int) -> None:
        self.connect_calls.append((host, port))

    def loop_forever(self) -> None:
        self.loop_forever_called = True

    def subscribe(self, topic: str, qos: int) -> None:
        self.subscriptions.append((topic, qos))


def build_service(
    *,
    recorder: Recorder | None = None,
    writers: dict[str, FakeWriter] | None = None,
    client: FakeClient | None = None,
) -> tuple[MQTTToPostgresService, Recorder, dict[str, FakeWriter], FakeClient]:
    config = build_config()
    recorder = recorder or Recorder()
    client = client or FakeClient()
    writers = writers or {route.table_name: FakeWriter() for route in config.routes}

    def writer_factory(*, route, config):
        return writers[route.table_name]

    def mqtt_client_factory(*, config, on_connect, on_message, on_disconnect):
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        return client

    service = MQTTToPostgresService(
        config=config,
        event_logger=recorder,
        writer_factory=writer_factory,
        mqtt_client_factory=mqtt_client_factory,
    )
    return service, recorder, writers, client


def test_topic_matches_single_level_wildcard() -> None:
    assert topic_matches("devices/+/temp", "devices/node-1/temp") is True
    assert topic_matches("devices/+/temp", "devices/node-1/humidity") is False


def test_topic_matches_multi_level_wildcard() -> None:
    assert topic_matches("$SYS/broker/#", "$SYS/broker/clients/total") is True
    assert topic_matches("$SYS/broker/#", "$SYS/other") is False


def test_service_logs_unique_subscriptions_on_connect() -> None:
    service, recorder, _, client = build_service()

    service.on_connect(client, None, None, 0)

    assert client.subscriptions == [("devices/+/temp", 0), ("$SYS/broker/#", 0)]
    assert [event for event, _ in recorder.events].count("mqtt.subscribed") == 2
    assert ("service.started",) == (recorder.events[-1][0],)


def test_service_logs_unrouted_message() -> None:
    service, recorder, _, _ = build_service()

    message = SimpleNamespace(topic="devices/node-1/humidity", payload=b"42")
    service.on_message(None, None, message)

    assert [event for event, _ in recorder.events] == [
        "message.received",
        "message.unrouted",
    ]


def test_service_logs_successful_database_write() -> None:
    service, recorder, writers, _ = build_service()

    payload = build_trace_payload(
        trace_id="trace-1",
        event_id="event-1",
        publisher_id="publisher-1",
        sequence=1,
        published_at=datetime.now(timezone.utc),
        value=42.0,
    )
    message = SimpleNamespace(topic="devices/node-1/temp", payload=payload.encode("utf-8"))
    service.on_message(None, None, message)

    assert writers["tbl_temp"].calls[0]["payload"] == "42.0"
    assert writers["tbl_temp"].calls[0]["trace"].trace_id == "trace-1"
    assert [event for event, _ in recorder.events] == [
        "message.received",
        "message.routed",
        "db.insert_attempted",
        "db.write_succeeded",
    ]


def test_service_logs_failed_database_write_and_continues() -> None:
    failing_writers = {
        "tbl_temp": FakeWriter(should_fail=True),
        "tbl_sys": FakeWriter(),
    }
    service, recorder, _, _ = build_service(writers=failing_writers)

    payload = build_trace_payload(
        trace_id="trace-1",
        event_id="event-1",
        publisher_id="publisher-1",
        sequence=1,
        published_at=datetime.now(timezone.utc),
        value=42.0,
    )
    message = SimpleNamespace(topic="devices/node-1/temp", payload=payload.encode("utf-8"))
    service.on_message(None, None, message)

    assert [event for event, _ in recorder.events] == [
        "message.received",
        "message.routed",
        "db.insert_attempted",
        "db.write_failed",
    ]


def test_service_logs_disconnect() -> None:
    service, recorder, _, _ = build_service()

    service.on_disconnect(None, None, 1)

    assert recorder.events[0][0] == "mqtt.disconnected"

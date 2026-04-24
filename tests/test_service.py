from pathlib import Path
from types import SimpleNamespace

from mqtt2postgres.config import AppConfig
from mqtt2postgres.contracts import BrokerContract, BrokerServer, DerivedContract, PostgresServer
from mqtt2postgres.service import MQTTToPostgresService, topic_matches


def build_broker_contract() -> BrokerContract:
    return BrokerContract(
        path=Path("contracts/raw/broker.odcs.yaml"),
        name="Broker Raw",
        version="1.0.0",
        contract_id="urn:mqtt:broker:raw",
        server=BrokerServer(
            name="mqtt-prod",
            protocol="mqtt",
            host="localhost",
            port=1883,
            qos=0,
            topic_filters=("devices/+/temp", "$SYS/broker/#"),
        ),
        model_name="mqtt_message",
        fields=("topic", "payload", "received_at"),
    )


def build_derived_contract(table_name: str, topic_filter: str) -> DerivedContract:
    return DerivedContract(
        path=Path(f"contracts/derived/{table_name}.odcs.yaml"),
        name=table_name,
        version="1.0.0",
        contract_id=f"urn:mqtt2postgres:{table_name}",
        server=PostgresServer(
            name="postgres-prod",
            host="localhost",
            port=5432,
            database="mqtt",
            schema="public",
        ),
        table_name=table_name,
        fields=("msg_date", "msg_topic", "msg_value"),
        source_contract_id="urn:mqtt:broker:raw",
        source_topic_filters=(topic_filter,),
    )


def build_config() -> AppConfig:
    return AppConfig(
        db_username="postgres",
        db_password="secret",
        mqtt_username=None,
        mqtt_password=None,
        mqtt_client_id="mqtt2postgres",
        log_format="json",
        log_level="INFO",
        config_snapshot_path=Path("/tmp/mqtt2postgres-snapshot.json"),
        broker_contract=build_broker_contract(),
        derived_contracts=(
            build_derived_contract("tbl_temp", "devices/+/temp"),
            build_derived_contract("tbl_sys", "$SYS/broker/#"),
        ),
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

    def insert_message(self, *, topic: str, payload: str, message_time) -> None:
        self.calls.append(
            {"topic": topic, "payload": payload, "message_time": message_time}
        )
        if self.should_fail:
            raise RuntimeError("db write failed")

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
    writers = writers or {
        str(contract.path): FakeWriter() for contract in config.derived_contracts
    }

    def writer_factory(*, contract, username, password):
        return writers[str(contract.path)]

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

    message = SimpleNamespace(topic="devices/node-1/temp", payload=b"42")
    service.on_message(None, None, message)

    assert writers["contracts/derived/tbl_temp.odcs.yaml"].calls[0]["payload"] == "42"
    assert [event for event, _ in recorder.events] == [
        "message.received",
        "message.routed",
        "db.write_succeeded",
    ]


def test_service_logs_failed_database_write_and_continues() -> None:
    failing_writers = {
        "contracts/derived/tbl_temp.odcs.yaml": FakeWriter(should_fail=True),
        "contracts/derived/tbl_sys.odcs.yaml": FakeWriter(),
    }
    service, recorder, _, _ = build_service(writers=failing_writers)

    message = SimpleNamespace(topic="devices/node-1/temp", payload=b"42")
    service.on_message(None, None, message)

    assert [event for event, _ in recorder.events] == [
        "message.received",
        "message.routed",
        "db.write_failed",
    ]


def test_service_logs_disconnect() -> None:
    service, recorder, _, _ = build_service()

    service.on_disconnect(None, None, 1)

    assert recorder.events[0][0] == "mqtt.disconnected"

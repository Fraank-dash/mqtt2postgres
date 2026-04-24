import json
import logging
from pathlib import Path

from mqtt2postgres.contracts import BrokerContract, BrokerServer, DerivedContract, PostgresServer
from mqtt2postgres.runtime_logging import (
    EventLogger,
    RuntimeEvent,
    build_config_snapshot,
    emit_snapshot_events,
    load_snapshot,
    redact_value,
    render_text_event,
    save_snapshot,
)


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
            topic_filters=("devices/+/temp",),
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


class ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def emit(self, event: str, **kwargs) -> None:
        self.events.append((event, kwargs))


def test_event_logger_serializes_json_event() -> None:
    logger = logging.getLogger("mqtt2postgres.test.runtime_logging")
    logger.handlers = []
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = ListHandler()
    logger.addHandler(handler)
    event_logger = EventLogger(
        logger=logger,
        broker_contract=build_broker_contract(),
        run_id="run-123",
    )

    event_logger.emit(
        "service.starting",
        component="service",
        message="Starting service.",
        details={"payload_size": 2},
    )

    payload = json.loads(handler.messages[0])
    assert payload["event"] == "service.starting"
    assert payload["component"] == "service"
    assert payload["run_id"] == "run-123"
    assert payload["details"]["payload_size"] == 2


def test_redaction_middleware_removes_payload_fields() -> None:
    sanitized = redact_value(
        {
            "payload": "secret",
            "nested": {
                "msg_value": "42",
                "safe": True,
            },
        }
    )

    assert sanitized == {"nested": {"safe": True}}


def test_render_text_event_is_human_readable() -> None:
    rendered = render_text_event(
        EventLogger(logger=logging.getLogger("mqtt2postgres.test.text")).middlewares[0](
            RuntimeEvent(
                event="mqtt.connected",
                component="mqtt",
                message="Connected to MQTT broker.",
                level="INFO",
                status="ok",
                topic="devices/node-1/temp",
                table="tbl_temp",
                broker_contract_id="urn:mqtt:broker:raw",
                derived_contract_id="urn:mqtt2postgres:tbl_temp",
                details={"payload_size": 12},
            )
        )
    )

    assert "mqtt.connected" in rendered
    assert "topic=devices/node-1/temp" in rendered
    assert "table=tbl_temp" in rendered
    assert "payload_size=12" in rendered


def test_event_logger_serializes_text_event() -> None:
    logger = logging.getLogger("mqtt2postgres.test.runtime_logging.text")
    logger.handlers = []
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = ListHandler()
    logger.addHandler(handler)
    event_logger = EventLogger(
        logger=logger,
        broker_contract=build_broker_contract(),
        run_id="run-123",
        log_format="text",
    )

    event_logger.emit(
        "service.starting",
        component="service",
        message="Starting service.",
        details={"payload_size": 2},
    )

    assert "service.starting" in handler.messages[0]
    assert "payload_size=2" in handler.messages[0]


def test_snapshot_round_trip(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "state" / "snapshot.json"
    snapshot = build_config_snapshot(
        build_broker_contract(),
        (build_derived_contract("tbl_temp", "devices/+/temp"),),
    )

    save_snapshot(snapshot_path, snapshot)

    assert load_snapshot(snapshot_path) == snapshot


def test_load_snapshot_returns_none_for_missing_file(tmp_path: Path) -> None:
    assert load_snapshot(tmp_path / "missing.json") is None


def test_emit_snapshot_events_reports_add_remove_and_change() -> None:
    broker = build_broker_contract()
    current = build_config_snapshot(
        broker,
        (
            build_derived_contract("tbl_temp", "devices/+/temp"),
            build_derived_contract("tbl_sys", "$SYS/broker/#"),
        ),
    )
    previous = build_config_snapshot(
        broker,
        (
            build_derived_contract("tbl_temp", "devices/+/humidity"),
            build_derived_contract("tbl_old", "devices/+/old"),
        ),
    )
    recorder = Recorder()

    emit_snapshot_events(
        recorder,
        current_snapshot=current,
        previous_snapshot=previous,
    )

    assert sorted(event for event, _ in recorder.events) == [
        "derived_contract.added",
        "derived_contract.changed",
        "derived_contract.removed",
    ]

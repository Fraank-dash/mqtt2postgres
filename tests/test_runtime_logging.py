import json
import logging

from observability.logging import (
    EventLogger,
    RuntimeEvent,
    redact_value,
    render_text_event,
)


class ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def test_event_logger_serializes_json_event() -> None:
    logger = logging.getLogger("mqtt2postgres.test.runtime_logging")
    logger.handlers = []
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = ListHandler()
    logger.addHandler(handler)
    event_logger = EventLogger(
        logger=logger,
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

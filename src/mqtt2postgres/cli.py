from __future__ import annotations

import sys

from mqtt2postgres.config import load_config
from mqtt2postgres.runtime_logging import EventLogger


def main(argv: list[str] | None = None) -> int:
    config = load_config(argv)
    from mqtt2postgres.service import MQTTToPostgresService

    event_logger = EventLogger(
        log_format=config.log_format,
        log_level=config.log_level,
    )
    event_logger.emit(
        "service.starting",
        component="service",
        message="Starting MQTT to Postgres service.",
        status="starting",
        details={
            "mqtt_host": config.mqtt_host,
            "mqtt_port": config.mqtt_port,
            "db_host": config.db_host,
            "db_port": config.db_port,
            "db_name": config.db_name,
            "db_schema": config.db_schema,
            "topic_filters": list(config.topic_filters),
            "db_ingest_function": config.db_ingest_function,
        },
    )

    try:
        service = MQTTToPostgresService(config, event_logger=event_logger)
        service.start()
    except KeyboardInterrupt:
        event_logger.emit(
            "service.stopping",
            component="service",
            message="Stopping MQTT to Postgres service.",
            status="stopping",
            details={"reason": "keyboard_interrupt"},
        )
        event_logger.emit(
            "service.stopped",
            component="service",
            message="MQTT to Postgres service stopped.",
            status="stopped",
        )
        return 0
    except Exception as exc:
        event_logger.emit(
            "service.stopping",
            component="service",
            message="Stopping MQTT to Postgres service after an unrecoverable error.",
            level="ERROR",
            status="failed",
            details={
                "error_class": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        event_logger.emit(
            "service.stopped",
            component="service",
            message="MQTT to Postgres service stopped after an unrecoverable error.",
            level="ERROR",
            status="failed",
        )
        raise
    event_logger.emit(
        "service.stopped",
        component="service",
        message="MQTT to Postgres service stopped.",
        status="stopped",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

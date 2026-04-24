from __future__ import annotations

import sys

from mqtt2postgres.config import load_config
from mqtt2postgres.runtime_logging import (
    EventLogger,
    build_config_snapshot,
    emit_snapshot_events,
    load_snapshot,
    save_snapshot,
)


def main(argv: list[str] | None = None) -> int:
    config = load_config(argv)
    from mqtt2postgres.service import MQTTToPostgresService

    event_logger = EventLogger(
        log_format=config.log_format,
        log_level=config.log_level,
        broker_contract=config.broker_contract,
    )
    event_logger.emit(
        "service.starting",
        component="service",
        message="Starting MQTT to Postgres service.",
        broker_contract=config.broker_contract,
        status="starting",
        details={
            "broker_contract_path": str(config.broker_contract.path),
            "derived_contract_paths": [str(contract.path) for contract in config.derived_contracts],
            "config_snapshot_path": str(config.config_snapshot_path),
        },
    )

    current_snapshot = build_config_snapshot(config.broker_contract, config.derived_contracts)
    try:
        previous_snapshot = load_snapshot(config.config_snapshot_path)
    except Exception as exc:
        previous_snapshot = None
        event_logger.emit(
            "config.snapshot.loaded",
            component="config",
            message="Failed to load the previous config snapshot.",
            level="ERROR",
            broker_contract=config.broker_contract,
            status="failed",
            details={
                "path": str(config.config_snapshot_path),
                "error_class": type(exc).__name__,
                "error_message": str(exc),
            },
        )
    else:
        if previous_snapshot is None:
            event_logger.emit(
                "config.snapshot.missing",
                component="config",
                message="No previous config snapshot was found.",
                broker_contract=config.broker_contract,
                status="missing",
                details={"path": str(config.config_snapshot_path)},
            )
        else:
            event_logger.emit(
                "config.snapshot.loaded",
                component="config",
                message="Loaded previous config snapshot.",
                broker_contract=config.broker_contract,
                status="loaded",
                details={"path": str(config.config_snapshot_path)},
            )
    emit_snapshot_events(
        event_logger,
        current_snapshot=current_snapshot,
        previous_snapshot=previous_snapshot,
    )

    try:
        save_snapshot(config.config_snapshot_path, current_snapshot)
    except Exception as exc:
        event_logger.emit(
            "config.snapshot.saved",
            component="config",
            message="Failed to save the current config snapshot.",
            level="ERROR",
            broker_contract=config.broker_contract,
            status="failed",
            details={
                "path": str(config.config_snapshot_path),
                "error_class": type(exc).__name__,
                "error_message": str(exc),
            },
        )
    else:
        event_logger.emit(
            "config.snapshot.saved",
            component="config",
            message="Saved the current config snapshot.",
            broker_contract=config.broker_contract,
            status="saved",
            details={"path": str(config.config_snapshot_path)},
        )

    try:
        service = MQTTToPostgresService(config, event_logger=event_logger)
        service.start()
    except KeyboardInterrupt:
        event_logger.emit(
            "service.stopping",
            component="service",
            message="Stopping MQTT to Postgres service.",
            broker_contract=config.broker_contract,
            status="stopping",
            details={"reason": "keyboard_interrupt"},
        )
        event_logger.emit(
            "service.stopped",
            component="service",
            message="MQTT to Postgres service stopped.",
            broker_contract=config.broker_contract,
            status="stopped",
        )
        return 0
    except Exception as exc:
        event_logger.emit(
            "service.stopping",
            component="service",
            message="Stopping MQTT to Postgres service after an unrecoverable error.",
            level="ERROR",
            broker_contract=config.broker_contract,
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
            broker_contract=config.broker_contract,
            status="failed",
        )
        raise
    event_logger.emit(
        "service.stopped",
        component="service",
        message="MQTT to Postgres service stopped.",
        broker_contract=config.broker_contract,
        status="stopped",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

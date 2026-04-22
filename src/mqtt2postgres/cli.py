from __future__ import annotations

import sys

from mqtt2postgres.config import load_config


def main(argv: list[str] | None = None) -> int:
    config = load_config(argv)
    from mqtt2postgres.service import MQTTToPostgresService

    service = MQTTToPostgresService(config)
    try:
        service.start()
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())

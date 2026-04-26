from __future__ import annotations

import sys
from collections.abc import Sequence

from broker.publisher.config import build_argument_parser, config_from_args, load_publisher_configs
from broker.publisher.models import PublisherError
from broker.publisher.runtime import run_publisher, run_publishers


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        if args.config:
            configs = load_publisher_configs(args.config)
            run_publishers(configs)
        else:
            config = config_from_args(args)
            run_publisher(config)
    except KeyboardInterrupt:
        print("Publisher stopped.")
        return 0
    except PublisherError as exc:
        parser.error(str(exc))
    except Exception as exc:
        print(f"Publisher failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

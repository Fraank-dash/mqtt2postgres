from __future__ import annotations

import warnings
from typing import Sequence

from mqtt2postgres.app import main as app_main


def main(argv: Sequence[str] | None = None) -> int:
    warnings.warn(
        "mqtt2postgres.cli is deprecated and will be removed in a future release. "
        "Use mqtt2postgres.app or `python -m mqtt2postgres` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return app_main(list(argv) if argv is not None else None)


if __name__ == "__main__":
    raise SystemExit(main())

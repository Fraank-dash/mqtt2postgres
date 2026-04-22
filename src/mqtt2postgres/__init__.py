"""MQTT to Postgres ingestor."""

from importlib.metadata import PackageNotFoundError, version

__all__ = ["__version__"]

try:
    __version__ = version("mqtt2postgres")
except PackageNotFoundError:
    __version__ = "0.0.0"

from importlib import import_module
from pathlib import Path


def test_publisher_and_subscriber_packages_share_core_module_layout() -> None:
    src_root = Path(__file__).resolve().parents[1] / "src" / "apps"
    expected_modules = {
        "__init__.py",
        "__main__.py",
        "cli.py",
        "models.py",
        "runtime.py",
        "settings.py",
    }

    publisher_modules = {path.name for path in (src_root / "publisher").iterdir() if path.is_file()}
    subscriber_modules = {path.name for path in (src_root / "subscriber").iterdir() if path.is_file()}

    assert expected_modules <= publisher_modules
    assert expected_modules == subscriber_modules


def test_canonical_entrypoint_modules_import_cleanly() -> None:
    subscriber_main = import_module("mqtt2postgres.__main__")
    publisher_main = import_module("apps.publisher.__main__")
    twin_config = import_module("apps.publisher.twin_config")

    assert callable(subscriber_main.main)
    assert callable(publisher_main.main)
    assert callable(twin_config.main)

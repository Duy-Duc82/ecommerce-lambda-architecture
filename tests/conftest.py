"""Shared fixtures, and the marker each test folder carries."""
from pathlib import Path

import pytest

from tests.spark_support import _release_cached_frames, spark  # noqa: F401

_FOLDER_MARKERS = {"spark": "spark", "integration": "integration", "legacy": "legacy", "drills": "drill"}
_TESTS = Path(__file__).resolve().parent


def pytest_collection_modifyitems(config, items):
    for item in items:
        try:
            folder = Path(str(item.fspath)).resolve().relative_to(_TESTS).parts[0]
        except ValueError:
            continue
        marker = _FOLDER_MARKERS.get(folder)
        if marker:
            item.add_marker(getattr(pytest.mark, marker))

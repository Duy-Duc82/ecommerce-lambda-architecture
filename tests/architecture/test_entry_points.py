"""Every console script in pyproject.toml names a callable that exists.

``ecomdata-ops`` once pointed at ``ecomdata.ops.cli:main``, a module that no longer
existed; nothing noticed because nothing ran the script.
"""
from __future__ import annotations

import importlib
import tomllib

import pytest

from tests.paths import REPO_ROOT

SCRIPTS = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["scripts"]


@pytest.mark.parametrize("name", sorted(SCRIPTS))
def test_a_console_script_resolves_to_a_callable(name):
    module_name, _, attribute = SCRIPTS[name].partition(":")

    target = getattr(importlib.import_module(module_name), attribute)

    assert callable(target)


def test_the_batch_stages_are_reachable_as_a_console_script():
    assert SCRIPTS["ecomdata-batch"] == "ecomdata.processing.batch.cli:main"

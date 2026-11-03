"""The dependency rules of plan section 2.2, enforced on every run."""
from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

from tests.paths import REPO_ROOT

SRC = REPO_ROOT / "src" / "ecomdata"
# Where a module may read the loaded settings: a composition root.
COMPOSITION_STEMS = {"bootstrap", "cli", "__main__"}


def test_import_linter_contracts_hold():
    result = subprocess.run(
        [sys.executable, "-c", "from importlinter.cli import lint_imports_command as m; m()"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _imports_settings(path: Path) -> list[int]:
    lines = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("ecomdata.settings"):
            lines.append(node.lineno)
        elif isinstance(node, ast.ImportFrom) and node.module == "ecomdata" and any(a.name == "settings" for a in node.names):
            lines.append(node.lineno)
        elif isinstance(node, ast.Import) and any(a.name.startswith("ecomdata.settings") for a in node.names):
            lines.append(node.lineno)
    return lines


def _is_composition_root(relative: str) -> bool:
    path = Path(relative)
    return (
        path.stem in COMPOSITION_STEMS
        or path.stem.endswith("service")
        or path.parts[0] == "orchestration"
        or path.parts[0] == "settings"
    )


def test_settings_are_imported_only_by_composition_roots():
    offenders = []
    for path in sorted(SRC.rglob("*.py")):
        relative = path.relative_to(SRC).as_posix()
        if _is_composition_root(relative):
            continue
        offenders += [f"{relative}:{line}" for line in _imports_settings(path)]
    assert offenders == []


def test_importing_the_settings_package_reads_no_environment(monkeypatch):
    code = (
        "import os, sys\n"
        "os.environ.pop('POSTGRES_PORT', None)\n"
        "os.environ['POSTGRES_PORT'] = 'not-a-number'\n"
        "import ecomdata.settings\n"
        "print('imported')\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120, cwd=REPO_ROOT)
    assert result.returncode == 0, result.stderr
    assert "imported" in result.stdout

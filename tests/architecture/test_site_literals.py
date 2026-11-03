"""Site-specific facts live in one place per concern (plan section 2.2)."""
from __future__ import annotations

import ast
import re
from pathlib import Path

from tests.paths import REPO_ROOT

SRC = REPO_ROOT / "src" / "ecomdata"
SITES = ("tiki", "lazada")
LITERAL = re.compile(r"\b(?:" + "|".join(SITES) + r")(?:\.vn)?\b", re.IGNORECASE)

# Where a site's code may name the site.
ALLOWED = (
    "ingestion/sources/",
    "processing/normalize/sources/",
    "domain/sources.py",
)
# Operator tools drive a whole stack, drills name the sources they exercise.
EXEMPT = ("ops/",)


def _allowed(relative: str) -> bool:
    return relative.startswith(ALLOWED + EXEMPT)


def _string_literals(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and not _is_docstring(tree, node):
            yield node.lineno, node.value


def _is_docstring(tree: ast.Module, node: ast.Constant) -> bool:
    for parent in ast.walk(tree):
        if isinstance(parent, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            body = parent.body
            if body and isinstance(body[0], ast.Expr) and body[0].value is node:
                return True
    return False


def test_a_site_name_appears_as_a_string_literal_only_in_its_own_modules():
    offenders = []
    for path in sorted(SRC.rglob("*.py")):
        relative = path.relative_to(SRC).as_posix()
        if _allowed(relative):
            continue
        for line, value in _string_literals(path):
            if LITERAL.search(value):
                offenders.append(f"{relative}:{line}: {value[:60]!r}")
    assert offenders == []

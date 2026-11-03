"""Filesystem anchors for tests: the repository root and the shared fixtures."""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures"

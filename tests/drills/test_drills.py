"""Phase 8 plan section 10: failure drills D1-D11 (test 32), marker `drill`.

Each drill runs in its own process, so the lake profile below is in effect
before config.settings is first imported; the rest of the host-side
addresses come from .env. The stack must already have passed `mp smoke`:
a drill fails, never skips, when its baseline validate does not pass.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.drill


# D11 last: it is the only drill that takes the whole stack down, so running
# it in the middle would make every later drill start from a cold stack.
@pytest.mark.parametrize("name", ["d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10", "d11"])
def test_drill(name):
    env = {**os.environ, "DATA_LAKE_PROFILE": "minio"}
    result = subprocess.run([sys.executable, "-m", "ops.drills", name], cwd=ROOT, env=env,
                            capture_output=True, text=True, timeout=5400)
    record = json.loads((ROOT / "data" / "ops" / "drills" / f"{name}.json").read_text(encoding="utf-8"))

    assert result.returncode == 0 and record["passed"], record.get("error") or result.stderr[-2000:]

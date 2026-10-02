"""Liveness for the long-running Python services (Phase 8 plan section 7.2).

A service touches its heartbeat file once per loop iteration. The container
healthcheck runs ``python -m common.heartbeat --check PATH --max-age S`` and
fails once the file is older than ``S``: a loop that hangs stops touching it,
while a process that merely stays alive does not count as healthy.

An empty path disables the heartbeat, so tests and host runs write nothing.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Callable


def heartbeat(path: str) -> Callable[[], None]:
    """A zero-argument callable that touches ``path``, or does nothing if it is empty."""
    if not path:
        return lambda: None

    def beat() -> None:
        # Opened and closed each time: an mtime is all the healthcheck reads.
        with open(path, "a", encoding="utf-8"):
            pass
        os.utime(path, None)

    return beat


def is_fresh(path: str, *, max_age_seconds: float, now: float | None = None) -> bool:
    try:
        modified = os.path.getmtime(path)
    except OSError:
        # Never written: the service has not finished its first iteration.
        return False
    return (time.time() if now is None else now) - modified <= max_age_seconds


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Container healthcheck for a service heartbeat file")
    parser.add_argument("--check", required=True, metavar="PATH")
    parser.add_argument("--max-age", required=True, type=float, metavar="SECONDS")
    args = parser.parse_args(argv)
    return 0 if is_fresh(args.check, max_age_seconds=args.max_age) else 1


if __name__ == "__main__":
    sys.exit(main())

"""Graceful shutdown for the long-running pipeline services."""
from __future__ import annotations

import signal
import threading


class StopSignal:
    """Set once, by a signal handler; every wait returns as soon as it is."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, seconds: float) -> bool:
        """Wait up to ``seconds``; True if the service should stop."""
        return self._event.wait(seconds)


def install_signal_handlers(stop: StopSignal) -> None:
    """SIGTERM (``docker stop``) and SIGINT (Ctrl+C) both ask for a clean stop."""

    def handler(signum, frame) -> None:  # noqa: ARG001 - the signal API's shape
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, handler)

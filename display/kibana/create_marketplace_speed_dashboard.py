"""Provision the marketplace Kibana dashboards.

Phase 5 left this script holding a data view and an empty dashboard shell.
Phase 8 WP8 replaced both with a committed saved-object export covering the
realtime changes dashboard and the source-health dashboard, so this is now a
thin call into that importer — kept because the Phase 5 runbook and
`start_all.ps1` name it.
"""
from __future__ import annotations

import sys

from display.kibana.setup_marketplace_kibana import main as setup_main


def create_marketplace_speed_dashboard() -> None:
    setup_main()


if __name__ == "__main__":
    sys.exit(setup_main())

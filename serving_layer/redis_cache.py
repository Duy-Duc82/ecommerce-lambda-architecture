"""Serving layer — Redis KPI access.

Read helpers over the `rt:kpi:*` keys written by the speed layer. Used by any
low-latency API / counter surface. Kibana reads Elasticsearch directly; Redis
holds only the latest realtime KPIs.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import redis

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import REDIS_DB, REDIS_HOST, REDIS_PORT

EVENT_TYPES = ("view", "cart", "purchase")


def get_redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def get_latest_kpis() -> dict[str, dict]:
    """Latest per-event-type window metrics."""
    r = get_redis()
    result = {}
    for event_type in EVENT_TYPES:
        data = r.hgetall(f"rt:kpi:{event_type}")
        if data:
            result[event_type] = {
                "window_start": data.get("window_start", ""),
                "event_count": int(data.get("event_count", 0)),
                "unique_users": int(data.get("unique_users", 0)),
                "revenue": float(data.get("revenue", 0.0)),
            }
    return result


def get_revenue_series(limit: int = 60) -> list[dict]:
    """Recent per-window purchase revenue points (newest first)."""
    r = get_redis()
    return [json.loads(item) for item in r.lrange("rt:kpi:revenue_series", 0, limit - 1)]


def get_cache_stats() -> dict:
    r = get_redis()
    return {
        "total_keys": r.dbsize(),
        "kpi_keys": len(list(r.scan_iter("rt:kpi:*"))),
        "used_memory": r.info("memory").get("used_memory_human", "N/A"),
    }


if __name__ == "__main__":
    print("=" * 50)
    print("REDIS KPI STATUS")
    print("=" * 50)
    for k, v in get_cache_stats().items():
        print(f"  {k}: {v}")
    print("\n--- Latest KPIs ---")
    for event_type, kpi in get_latest_kpis().items():
        print(f"  {event_type}: {kpi}")
    print("\n--- Revenue series (recent) ---")
    for point in get_revenue_series(10):
        print(f"  {point['t']}: {point['revenue']:,.2f}")

"""
Serving Layer — Redis Cache Manager.

Cung cap cac ham tien ich de doc/ghi du lieu real-time tu Redis.
Duoc su dung boi Dashboard va API.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import redis

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import REDIS_DB, REDIS_HOST, REDIS_PORT


def get_redis() -> redis.Redis:
    """Tao ket noi Redis."""
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True,
    )


# ============================================================
# REAL-TIME METRICS
# ============================================================

def get_realtime_event_counts(pattern: str = "realtime:count:*") -> dict[str, int]:
    """Lay so luong event theo loai tu Redis."""
    r = get_redis()
    result = {}
    for key in r.scan_iter(pattern):
        value = r.get(key)
        if value is not None:
            # Extract metric name tu key
            metric_name = key.replace("realtime:count:", "")
            result[metric_name] = int(value)
    return result


def get_realtime_revenue(limit: int = 12) -> list[dict]:
    """Lay doanh thu real-time theo cac cua so 5 phut."""
    r = get_redis()
    result = []

    keys = sorted(r.keys("realtime:revenue_5min:*"), reverse=True)[:limit]
    for key in keys:
        data = r.hgetall(key)
        if data:
            window_start = key.replace("realtime:revenue_5min:", "")
            result.append({
                "window_start": window_start,
                "total_revenue": float(data.get("total_revenue", 0)),
                "order_count": int(data.get("order_count", 0)),
                "unique_buyers": int(data.get("unique_buyers", 0)),
                "avg_order_value": float(data.get("avg_order_value", 0)),
            })

    return result


def get_fraud_alerts(limit: int = 50) -> list[dict]:
    """Lay cac canh bao gian lan gan nhat."""
    r = get_redis()
    alerts = r.lrange("alerts:fraud", 0, limit - 1)
    return [json.loads(a) for a in alerts]


def get_top_products(event_type: str = "page_view", limit: int = 10) -> list[dict]:
    """Lay top san pham theo loai event."""
    r = get_redis()
    result = []

    for key in r.scan_iter(f"realtime:count:*:{event_type}"):
        value = r.get(key)
        if value:
            product = key.replace("realtime:count:", "").replace(f":{event_type}", "")
            result.append({"product": product, "count": int(value)})

    result.sort(key=lambda x: x["count"], reverse=True)
    return result[:limit]


def get_active_users() -> int:
    """Lay so luong user dang hoat dong (tu realtime metrics)."""
    r = get_redis()
    total = 0
    for key in r.scan_iter("realtime:users:*"):
        value = r.get(key)
        if value:
            total += int(value)
    return total


# ============================================================
# CACHE MANAGEMENT
# ============================================================

def clear_realtime_cache() -> int:
    """Xoa tat ca cache real-time."""
    r = get_redis()
    keys = list(r.scan_iter("realtime:*"))
    if keys:
        r.delete(*keys)
    return len(keys)


def get_cache_stats() -> dict:
    """Lay thong ke ve Redis cache."""
    r = get_redis()
    info = r.info("memory")
    return {
        "total_keys": r.dbsize(),
        "realtime_keys": len(list(r.scan_iter("realtime:*"))),
        "alert_keys": r.llen("alerts:fraud"),
        "used_memory": info.get("used_memory_human", "N/A"),
        "connected_clients": r.info("clients").get("connected_clients", 0),
    }


# ============================================================
# MAIN (Test/Debug)
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("REDIS CACHE STATUS")
    print("=" * 50)

    stats = get_cache_stats()
    for k, v in stats.items():
        print(f"  {k}: {v}")

    print(f"\n--- Event Counts ---")
    counts = get_realtime_event_counts()
    for k, v in sorted(counts.items(), key=lambda x: -x[1])[:10]:
        print(f"  {k}: {v}")

    print(f"\n--- Revenue (5min windows) ---")
    revenue = get_realtime_revenue(5)
    for r in revenue:
        print(f"  {r['window_start']}: {r['total_revenue']:,.0f} VND ({r['order_count']} orders)")

    print(f"\n--- Fraud Alerts ---")
    alerts = get_fraud_alerts(5)
    for a in alerts:
        print(f"  [{a.get('alert_type')}] user={a.get('user_id')} amount={a.get('total_amount'):,.0f}")

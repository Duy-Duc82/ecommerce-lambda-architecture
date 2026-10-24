"""Measure the 8-hour full-stack run (2026-10-23 18:09Z .. 2026-10-24 02:10Z): scale, crawl method numbers
and per-layer latency, from Elasticsearch, Postgres, Kafka, MinIO and Loki. Writes run_report_metrics.json.

usage: python run_report_metrics.py <out.json>
"""
import json, subprocess, sys
from bisect import bisect_left
from datetime import datetime, timedelta, timezone

import numpy as np
import requests

ES, LOKI = "http://localhost:9201", "http://localhost:3101"
T0, T1 = "2026-10-23T18:09:00Z", "2026-10-24T02:10:00Z"


def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def pct(v, ps=(50, 95, 99)):
    v = np.array(v, dtype=float)
    return {f"p{p}": round(float(np.percentile(v, p)), 2) for p in ps} | {"max": round(float(v.max()), 2), "n": len(v)} if len(v) else {}


def scroll(index, field, src):
    out, body = [], {"size": 5000, "sort": [{field: "asc"}], "_source": src,
                     "query": {"range": {field: {"gte": T0, "lt": T1}}}}
    r = requests.post(f"{ES}/{index}/_search?scroll=2m", json=body, timeout=60).json()
    while r["hits"]["hits"]:
        out += [h["_source"] for h in r["hits"]["hits"]]
        r = requests.post(f"{ES}/_search/scroll", json={"scroll": "2m", "scroll_id": r["_scroll_id"]}, timeout=60).json()
    return out


def psql(sql):
    return subprocess.run(["docker", "exec", "th-postgres-dw", "psql", "-U", "admin", "-d", "data_warehouse", "-tAF", "|", "-c", sql],
                          capture_output=True, text=True, encoding="utf-8", errors="replace").stdout.strip()


def sh(cmd):
    return subprocess.run(["sh", "-c", cmd], capture_output=True, text=True, encoding="utf-8", errors="replace").stdout.strip()


m = {"window_utc": [T0, T1], "window_vn": ["2026-10-24 01:09", "2026-10-24 09:10"]}

# ---- crawl -------------------------------------------------------------------------------------------------
att = scroll("marketplace-crawl-attempts-*", "completed_at",
             ["task_id", "started_at", "completed_at", "status", "http_status", "latency_ms", "raw_bytes", "parsed_count", "rejected_count", "error_kind"])
m["crawl"] = dict(
    attempts=len(att), succeeded=sum(a["status"] == "SUCCEEDED" for a in att),
    http_status={str(k): sum(1 for a in att if a["http_status"] == k) for k in sorted({a["http_status"] for a in att}, key=str)},
    error_kinds={k: sum(1 for a in att if a["error_kind"] == k) for k in {a["error_kind"] for a in att} if k},
    distinct_targets=len({a["task_id"] for a in att}),
    parsed_offers_total=sum(a["parsed_count"] or 0 for a in att), rejected_total=sum(a["rejected_count"] or 0 for a in att),
    parsed_per_page=pct([a["parsed_count"] for a in att if a["status"] == "SUCCEEDED"]),
    raw_mb_total=round(sum(a["raw_bytes"] or 0 for a in att) / 2**20, 1),
    raw_kb_per_page=pct([(a["raw_bytes"] or 0) / 1024 for a in att if a["raw_bytes"]]),
    attempt_latency_ms=pct([a["latency_ms"] for a in att if a["latency_ms"] is not None]),
    pages_per_hour=round(len(att) / ((ts(T1) - ts(T0)).total_seconds() / 3600), 1),
)
by_task = {}
for a in att:
    by_task.setdefault(a["task_id"], []).append(ts(a["completed_at"]))
by_target = {}
for line in psql(f"select f.target, a.completed_at from audit.crawl_request_attempt a join audit.crawl_frontier f using (task_id) "
                 f"where a.completed_at >= '{T0}' and a.completed_at < '{T1}'").splitlines():
    tgt, done = line.split("|")
    by_target.setdefault(tgt, []).append(ts(done.replace(" ", "T")))
m["crawl"]["distinct_categories"] = len(by_target)
revisit = [(b - a).total_seconds() / 60 for v in by_target.values() for a, b in zip(sorted(v), sorted(v)[1:])]
m["crawl"]["revisit_minutes"] = pct(revisit)
starts = sorted(ts(a["started_at"]) for a in att)
m["crawl"]["gap_between_requests_s"] = pct([(b - a).total_seconds() for a, b in zip(starts, starts[1:])])

# ---- crawl -> Kafka (observation observed_at -> produced_at) and changes ------------------------------------
ch = scroll("marketplace-changes-*", "detected_at", ["change_type", "current_value", "detected_at"])
pub = []
for c in ch:
    try:
        v = json.loads(c["current_value"])
        pub.append((ts(v["produced_at"]) - ts(v["observed_at"])).total_seconds() * 1000)
    except (KeyError, TypeError, ValueError):
        pass
m["kafka_publish_ms"] = pct(pub)
m["changes"] = {k: sum(1 for c in ch if c["change_type"] == k) for k in sorted({c["change_type"] for c in ch})}

# ---- speed: micro-batches and crawl -> Elasticsearch -------------------------------------------------------
sb = scroll("marketplace-speed-batches-*", "started_at", ["started_at", "completed_at", "duration_ms", "input_rows", "change_rows", "es_rows", "redis_rows", "status", "invalid_rows", "late_rows"])
m["speed"] = dict(batches=len(sb), failed=sum(b["status"] != "SUCCEEDED" for b in sb), input_rows=sum(b["input_rows"] for b in sb),
                  change_rows=sum(b["change_rows"] for b in sb), es_rows=sum(b["es_rows"] for b in sb), redis_rows=sum(b["redis_rows"] for b in sb),
                  invalid_rows=sum(b["invalid_rows"] for b in sb), late_rows=sum(b["late_rows"] for b in sb),
                  duration_ms=pct([b["duration_ms"] for b in sb]), duration_ms_nonempty=pct([b["duration_ms"] for b in sb if b["input_rows"] > 0]),
                  trigger_interval_s=pct([(ts(b["started_at"]) - ts(a["started_at"])).total_seconds() for a, b in zip(sb, sb[1:])]))
nonempty = [b for b in sb if b["input_rows"] > 0]
bs = [ts(b["started_at"]) for b in nonempty]
e2e = []
for a in att:
    if a["status"] != "SUCCEEDED" or not a["parsed_count"]:
        continue
    i = bisect_left(bs, ts(a["completed_at"]))
    if i < len(nonempty):
        e2e.append((ts(nonempty[i]["completed_at"]) - ts(a["completed_at"])).total_seconds())
m["speed"]["crawl_done_to_es_s"] = pct(e2e)

# ---- Kafka -> Silver: sample silver_record log lines, compare with the Kafka record timestamp ----------------
q = requests.get(f"{LOKI}/loki/api/v1/query_range", params={"query": '{service="silver-sink"} |= "silver_record"', "limit": 40,
                                                            "start": int(ts("2026-10-24T01:00:00Z").timestamp() * 1e9),
                                                            "end": int(ts("2026-10-24T01:05:00Z").timestamp() * 1e9)}, timeout=60).json()
lat = []
for s in q["data"]["result"]:
    for t, line in s["values"][:40]:
        rec = json.loads(line[line.index("{"):])
        out = subprocess.run(["docker", "exec", "th-kafka", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:19092",
                              "--topic", "marketplace.observations.v1", "--partition", str(rec["partition"]), "--offset", str(rec["offset"]),
                              "--max-messages", "1", "--formatter-property", "print.timestamp=true", "--timeout-ms", "10000"],
                             capture_output=True, text=True, encoding="utf-8", errors="replace").stdout
        out = out[out.find("CreateTime:"):] if "CreateTime:" in out else ""  # a deprecation notice comes first
        if out.startswith("CreateTime:"):
            create_ms = int(out.split()[0].split(":")[1])
            lat.append(int(t) / 1e6 - create_ms)
            try:
                obs = json.loads(out[out.index("{"):])
                pub.append(create_ms - ts(obs["observed_at"]).timestamp() * 1000)
            except (ValueError, KeyError):
                pass
m["kafka_publish_ms_sample"] = pct(pub[-len(lat):]) if lat else {}
m["silver"] = dict(kafka_to_silver_ms_sample=pct(lat), consumer_lag=sh(
    "MSYS_NO_PATHCONV=1 docker exec th-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --group marketplace-silver-v1 --describe 2>/dev/null | awk 'NR>1 && $6!=\"\" {s+=$6} END {print s}'"),
    kafka_observations_total=sh("MSYS_NO_PATHCONV=1 docker exec th-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic marketplace.observations.v1 2>/dev/null | awk -F: '{s+=$3} END {print s}'"))

# ---- batch ----------------------------------------------------------------------------------------------------
runs = psql("select run_id, status, quality_status, as_of, started_at, silver_rows, gold_rows, mandatory_failure_count from audit.marketplace_batch_run order by as_of")
ticks = requests.get(f"{LOKI}/loki/api/v1/query_range", params={"query": '{service="batch-scheduler"} |= "batch_tick"', "limit": 100,
                                                               "start": int(ts("2026-10-23T18:00:00Z").timestamp() * 1e9)}, timeout=60).json()
tick_at = {}
for s in ticks["data"]["result"]:
    for t, line in s["values"]:
        try:
            rid = json.loads(line[line.index("{"):])["run_id"]
            when = datetime.fromtimestamp(int(t) / 1e9, timezone.utc)
            tick_at[rid] = max(tick_at.get(rid, when), when)
        except (ValueError, KeyError):
            pass
rows = []
for r in runs.splitlines():
    run_id, status, q_, as_of, started, silver, gold, mand = r.split("|")
    st, ao = ts(started.replace(" ", "T")), ts(as_of.replace(" ", "T"))
    done = tick_at.get(run_id)
    rows.append(dict(run_id=run_id, status=status, quality=q_, as_of=as_of, started=started, silver_rows=int(silver or 0), gold_rows=int(gold or 0),
                     mandatory_failures=int(mand or 0), duration_min=round((done - st).total_seconds() / 60, 1) if done else None,
                     as_of_to_published_min=round((done - ao).total_seconds() / 60, 1) if done else None))
m["batch"] = dict(runs=rows, interval_s=3600, lag_s=1200)
m["gold"] = dict(tables={t.split("|")[0]: int(t.split("|")[1]) for t in psql(
    "select relname, n_live_tup from pg_stat_user_tables where schemaname='cache' and relname like 'marketplace%' order by 1").splitlines()},
    offers=psql("select count(*) from cache.marketplace_offer_current"), sellers=psql("select count(*) from cache.marketplace_seller_current"),
    categories=psql("select count(distinct category_path) from cache.marketplace_offer_current"),
    newest_observation_vn=psql("select to_char(max(last_seen_at) at time zone 'Asia/Ho_Chi_Minh','YYYY-MM-DD HH24:MI') from cache.marketplace_offer_current"))

# ---- storage, logs, resources -----------------------------------------------------------------------------
m["storage"] = dict(minio=sh("MSYS_NO_PATHCONV=1 docker exec th-minio du -sh /data 2>/dev/null"),
                    minio_buckets=sh("docker exec th-minio sh -c 'du -sh /data/* 2>/dev/null'"),
                    es_indices=sh(f"curl -s '{ES}/_cat/indices/marketplace-*?h=index,docs.count,store.size&s=index'"),
                    postgres=psql("select pg_size_pretty(pg_database_size('data_warehouse'))"))
lq = requests.get(f"{LOKI}/loki/api/v1/query", params={"query": 'sum by (layer) (count_over_time({project="mp-tikihtml"}[8h]))', "time": int(ts(T1).timestamp())}, timeout=60).json()
le = requests.get(f"{LOKI}/loki/api/v1/query", params={"query": 'sum by (layer, level) (count_over_time({project="mp-tikihtml", level=~"error|warn|expected"}[8h]))', "time": int(ts(T1).timestamp())}, timeout=60).json()
m["logs"] = dict(lines={r["metric"]["layer"]: int(r["value"][1]) for r in lq["data"]["result"]},
                 by_level=[(r["metric"].get("layer"), r["metric"].get("level"), int(r["value"][1])) for r in le["data"]["result"]])
m["resources"] = dict(docker_stats=sh("docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' | grep '^th-' | sort"),
                      docker_vm=sh("docker info --format '{{.MemTotal}} {{.NCPU}}'"))
json.dump(m, open(sys.argv[1], "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=str)
print(json.dumps({k: v for k, v in m.items() if k not in ("batch", "storage", "resources")}, ensure_ascii=False, indent=1, default=str)[:6000])

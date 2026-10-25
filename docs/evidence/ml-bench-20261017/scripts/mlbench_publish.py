"""Analyse the crawled data and publish it: event-level results to Elasticsearch + a Kibana dashboard
(the speed-layer view), table-level results to Postgres + a Superset dashboard (the batch-layer view).

Cho Tot is analysed with statistics only (median/MAD, Kaplan-Meier): its robots.txt carries
Content-Signal ai-train=no. Machine-learning models (Isolation Forest, ECOD) run on Tiki only.

usage: python mlbench_publish.py <merged_raw_dir> <results_dir> <develop_worktree>
env:   ES (http://localhost:9204), KIBANA (http://localhost:5605), SUPERSET (http://localhost:8094),
       PG (postgresql+psycopg2://admin:admin123@localhost:5437/data_warehouse)
"""
import json, math, os, re, sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, text

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from chotot_anomaly_bench import FLAGS, MIN_GROUP, frame  # noqa: E402
from retail_bench import cusum, page_hinkley  # noqa: E402

ES = os.environ.get("ES", "http://localhost:9204")
KIBANA = os.environ.get("KIBANA", "http://localhost:5605")
SUPERSET = os.environ.get("SUPERSET", "http://localhost:8094")
PG = os.environ.get("PG", "postgresql+psycopg2://admin:admin123@localhost:5437/data_warehouse")
PG_FROM_SUPERSET = "postgresql+psycopg2://admin:admin123@postgres-dw:5432/data_warehouse"
SCHEMA = "mlbench"


def jl(path):
    return [json.loads(l) for l in open(path, encoding="utf-8")] if path.exists() else []


def yn(b):
    return "yes" if b else "no"


# ============================================================================
# Elasticsearch
# ============================================================================

def es_load(index, props, docs):
    requests.delete(f"{ES}/{index}", timeout=30)
    r = requests.put(f"{ES}/{index}", json={"mappings": {"properties": props}}, timeout=30)
    r.raise_for_status()
    for i in range(0, len(docs), 2000):
        body = "".join(json.dumps({"index": {}}) + "\n" + json.dumps(d, ensure_ascii=False, default=str) + "\n"
                       for d in docs[i:i + 2000])
        r = requests.post(f"{ES}/{index}/_bulk?refresh=true", data=body.encode("utf-8"),
                          headers={"Content-Type": "application/x-ndjson"}, timeout=120)
        r.raise_for_status()
        if r.json().get("errors"):
            raise RuntimeError(f"bulk errors in {index}: {json.dumps(r.json()['items'][:2])[:500]}")
    return len(docs)


K, D, F, I = ({"type": "keyword"}, {"type": "date"}, {"type": "double"}, {"type": "long"})
TXT = {"type": "text", "fields": {"raw": {"type": "keyword", "ignore_above": 256}}}


# ============================================================================
# Cho Tot (statistics only)
# ============================================================================

def chotot_listings(raw):
    """Score every streamed new phone listing against the baseline of listings crawled before it."""
    obs = []
    for r in jl(raw / "chotot_models.jsonl"):
        for it in r["items"]:
            obs.append(("model_page", r["ts"], None, it))
    for r in jl(raw / "chotot_stream.jsonl"):
        cat = r["url"].rsplit("/", 1)[-1]
        for it in r["items"]:
            obs.append(("stream", r["ts"], cat, it))
    obs.sort(key=lambda o: o[1])
    seen, full, model = set(), defaultdict(list), defaultdict(list)
    docs = []
    for kind, ts, cat, it in obs:
        lid = it["list_id"]
        if lid in seen:
            continue
        seen.add(lid)
        p = it.get("price") or 0
        params = it.get("params") or {}
        m, c = params.get("mobile_model"), params.get("mobile_capacity")
        if kind == "stream":
            level, base = "none", None
            if m and len(full[(m, c)]) >= MIN_GROUP:
                level, base = "model+capacity", full[(m, c)]
            elif m and len(model[m]) >= MIN_GROUP:
                level, base = "model", model[m]
            z = med = None
            if base is not None and p > 1000:
                arr = np.array(base)
                med_log = float(np.median(arr))
                mad = float(np.median(np.abs(arr - med_log)))
                med = math.exp(med_log)
                z = (math.log(p) - med_log) / (1.4826 * mad) if mad > 0 else None
            subj = (it.get("subject") or "")
            docs.append({"@timestamp": ts, "list_id": lid, "category": cat, "subject": subj,
                         "model": m, "capacity": c, "price": p or None, "dealer": yn(it.get("company_ad")),
                         "baseline_level": level, "baseline_n": len(base) if base is not None else 0,
                         "baseline_median": round(med) if med else None,
                         "price_ratio": round(p / med, 3) if med and p else None,
                         "robust_z": round(z, 2) if z is not None else None,
                         "flagged": yn(z is not None and abs(z) > 3.5),
                         "defect_in_title": yn(re.search(FLAGS["defect"], subj.lower())),
                         "day": ts[:10]})
        if m and p > 1000:
            full[(m, c)].append(math.log(p))
            model[m].append(math.log(p))
    baseline = []
    for (m, c), v in full.items():
        if len(v) >= MIN_GROUP:
            arr = np.array(v)
            med = float(np.median(arr))
            baseline.append(dict(model=m, capacity=c, listings=len(v), median_price=round(math.exp(med)),
                                 mad_log=round(float(np.median(np.abs(arr - med))), 4),
                                 p25=round(math.exp(np.percentile(arr, 25))), p75=round(math.exp(np.percentile(arr, 75)))))
    return docs, pd.DataFrame(baseline)


def chotot_lifecycle(raw, listing_docs):
    first = {}
    for r in jl(raw / "chotot_stream.jsonl"):
        for it in r["items"]:
            first.setdefault(it["list_id"], (r["ts"], r["url"].rsplit("/", 1)[-1], it.get("price")))
    events, last_alive = [], {}
    by = defaultdict(list)
    for c in jl(raw / "chotot_track.jsonl"):
        by[c["list_id"]].append(c)
    rows = []
    for lid, cs in by.items():
        if lid not in first:
            continue
        t0 = datetime.fromisoformat(first[lid][0]).timestamp()
        cs = sorted((c for c in cs if isinstance(c["status"], int) and not c["challenged"]), key=lambda c: c["ts"])
        start = min([c["list_time"] / 1000 for c in cs if c.get("list_time")] + [t0])
        ev = None
        alive = None
        for c in cs:
            if c["status"] == 410 or (c["status"] == 200 and c.get("ad_status") not in (None, "active")):
                ev = c
                break
            if c["alive"]:
                alive = c
        if ev is None and alive is None:
            continue
        end = datetime.fromisoformat((ev or alive)["ts"]).timestamp()
        rows.append(dict(list_id=lid, category=first[lid][1], T=max(end - start, 60) / 3600, E=int(ev is not None)))
        if ev:
            events.append({"@timestamp": ev["ts"], "list_id": lid, "category": first[lid][1], "price": first[lid][2],
                           "hours_since_posted": round((end - start) / 3600, 2),
                           "last_alive_at": alive["ts"] if alive else None, "status": str(ev["status"])})
    df = pd.DataFrame(rows)
    from lifelines import KaplanMeierFitter
    km_rows = []
    for cat, g in [("all", df)] + list(df.groupby("category")):
        km = KaplanMeierFitter().fit(g["T"], g["E"])
        tab = km.survival_function_.reset_index()
        tab.columns = ["t_hours", "survival"]
        for _, row in tab.iterrows():
            km_rows.append(dict(category=cat, t_hours=round(float(row["t_hours"]), 3), survival=round(float(row["survival"]), 4),
                                listings=len(g), removed=int(g["E"].sum())))
    return events, pd.DataFrame(km_rows)


# ============================================================================
# Retail: DMX and Tiki price series
# ============================================================================

def retail_series(raw, job):
    s = defaultdict(list)
    for r in jl(raw / f"{job}.jsonl"):
        if r["status"] != 200:
            continue
        cat = r["url"].rsplit("/", 1)[-1]
        for it in r["items"]:
            if it.get("price"):
                s[str(it["id"])].append((r["ts"], float(it["price"]), it, cat))
    out = {}
    for k, v in s.items():
        v.sort(key=lambda x: x[0])
        dedup = []
        for x in v:  # one observation per minute: the same offer can sit in two categories of one pass
            if not dedup or x[0][:16] != dedup[-1][0][:16]:
                dedup.append(x)
        out[k] = dedup
    return out


def price_change_docs(S, source):
    docs = []
    for k, v in S.items():
        x = [p for _, p, _, _ in v]
        if len(x) < 2:
            continue
        cu, ph = set(cusum(x)), set(page_hinkley(list(x)))
        for i in range(1, len(x)):
            if x[i] == x[i - 1]:
                continue
            ts, p, it, cat = v[i]
            docs.append({"@timestamp": ts, "source": source, "offer_id": k, "name": it.get("name"), "category": cat,
                         "old_price": x[i - 1], "new_price": p, "pct_change": round(100 * (p / x[i - 1] - 1), 2),
                         "direction": "up" if p > x[i - 1] else "down",
                         "rule_detected": "yes",
                         "cusum_detected": yn(any(a in cu for a in range(i, i + 4))),
                         "page_hinkley_detected": yn(any(a in ph for a in range(i, i + 4))),
                         "minutes_since_prev": round((datetime.fromisoformat(ts) - datetime.fromisoformat(v[i - 1][0])).total_seconds() / 60, 1)})
    return docs


def retail_offer_anomaly(S, source, w=12):
    """Batch rules over each offer's own history; returns flagged rows and per-method coverage."""
    rows, cov = [], defaultdict(lambda: [0, 0, 0])  # method -> [observations, scorable, flagged]
    for k, v in S.items():
        x = [p for _, p, _, _ in v]
        for i in range(5, len(x)):
            hist = np.array(x[max(0, i - w):i])
            med = float(np.median(hist))
            mad = float(np.median(np.abs(hist - med)))
            q1, q3 = np.percentile(hist, [25, 75])
            pct = x[i] / med - 1
            current = None
            if mad > 0:
                current = abs(x[i] - med) / (1.4826 * mad) > 3.5
            elif q3 > q1:
                current = x[i] < q1 - 1.5 * (q3 - q1) or x[i] > q3 + 1.5 * (q3 - q1)
            hampel = abs(x[i] - med) / (1.4826 * max(mad, 0.01 * med)) > 3
            b2 = abs(pct) > 0.15
            for name, flag in (("current_rule_mad_then_iqr", current), ("rolling_median_pct_15", b2), ("hampel_mad_floor", hampel)):
                c = cov[name]
                c[0] += 1
                if flag is not None:
                    c[1] += 1
                    c[2] += bool(flag)
            if b2 or hampel:
                nxt = x[i + 1] if i + 1 < len(x) else None
                kind = "unknown (last observation)" if nxt is None else \
                    "spike (back next observation)" if abs(nxt / med - 1) <= 0.02 else "level shift (stays)"
                ts, p, it, cat = v[i]
                rows.append(dict(source=source, observed_at=ts, offer_id=k, name=(it.get("name") or "")[:120], category=cat,
                                 price=p, rolling_median=med, pct_deviation=round(100 * pct, 2), flag_rolling_median_pct=b2,
                                 flag_hampel=hampel, current_rule_verdict="no verdict (MAD=IQR=0)" if current is None else yn(current),
                                 kind=kind))
    coverage = [dict(source=source, method=m, observations=c[0], scorable_share=round(c[1] / max(1, c[0]), 4),
                     flagged_share=round(c[2] / max(1, c[0]), 4)) for m, c in cov.items()]
    return rows, coverage


def tiki_ml(S, seeds=10):
    """Cross-sectional price anomaly on the latest Tiki observation of each product:
    rule (robust z within leaf category) vs Isolation Forest vs ECOD, with injected anomalies."""
    from pyod.models.ecod import ECOD
    from sklearn.ensemble import IsolationForest
    from sklearn.metrics import average_precision_score, roc_auc_score
    rows = []
    for k, v in S.items():
        ts, p, it, cat = v[-1]
        leaf = str(it.get("cat_path") or "").split("/")[-1] or cat
        rows.append(dict(offer_id=k, name=(it.get("name") or "")[:120], leaf=leaf, price=p, orig=it.get("orig") or p,
                         reviews=it.get("reviews") or 0, rating=it.get("rating") or 0, sold=it.get("sold") or 0))
    base = pd.DataFrame(rows)
    base = base[base.groupby("leaf")["price"].transform("size") >= MIN_GROUP].reset_index(drop=True)

    def feats(df):
        lp = np.log(df["price"])
        med = lp.groupby(df["leaf"]).transform("median")
        mad = (lp - med).abs().groupby(df["leaf"]).transform("median")
        z = ((lp - med) / (1.4826 * mad.replace(0, np.nan))).abs().fillna(0)
        X = np.column_stack([lp - med, (df["orig"] - df["price"]) / df["orig"].clip(lower=1), np.log1p(df["reviews"]),
                             df["rating"], np.log1p(df["sold"])])
        return z.values, X

    metrics = defaultdict(lambda: defaultdict(list))
    factors = [0.2, 0.35, 0.5, 2.0, 3.0, 5.0]
    for seed in range(seeds):
        rng = np.random.default_rng(seed)
        df = base.copy()
        inj = rng.choice(len(df), size=int(0.03 * len(df)), replace=False)
        df.loc[inj, "price"] = df.loc[inj, "price"] * rng.choice(factors, len(inj))
        y = np.zeros(len(df))
        y[inj] = 1
        z, X = feats(df)
        Xnd = np.delete(X, 1, axis=1)  # without the discount feature, which an injected price also shifts
        above = (df["price"] > df["orig"] * 1.001).values.astype(float)  # shown price above the struck-through price
        scores = {"rule_robust_z_in_category": z,
                  "rule_z_plus_price_above_original": z + 100 * above,
                  "isolation_forest": -IsolationForest(n_estimators=200, random_state=seed).fit(X).score_samples(X),
                  "isolation_forest_no_discount": -IsolationForest(n_estimators=200, random_state=seed).fit(Xnd).score_samples(Xnd),
                  "ecod": ECOD().fit(X).decision_scores_}
        for name, s in scores.items():
            metrics[name]["auc"].append(roc_auc_score(y, s))
            metrics[name]["ap"].append(average_precision_score(y, s))
    z, X = feats(base)
    base["robust_z"] = np.round(z, 2)
    base["iforest_score"] = np.round(-IsolationForest(n_estimators=200, random_state=0).fit(X).score_samples(X), 4)
    base["ecod_score"] = np.round(ECOD().fit(X).decision_scores_, 3)
    top = pd.concat([base.nlargest(100, c) for c in ("robust_z", "iforest_score", "ecod_score")]).drop_duplicates("offer_id")
    summary = {n: {m: (round(float(np.mean(v)), 4), round(float(np.std(v)), 4)) for m, v in ms.items()} for n, ms in metrics.items()}
    return top, summary, len(base), int(0.03 * len(base))


def tiki_demand(raw):
    """Pass-to-pass demand signal; a pass ends when a category URL comes round again."""
    passes, seen = [{}], set()
    starts = []
    for r in jl(raw / "tiki.jsonl"):
        if r["url"] in seen:
            passes.append({})
            seen = set()
        if not seen:
            starts.append(r["ts"])
        seen.add(r["url"])
        if r["status"] == 200:
            for it in r["items"]:
                passes[-1].setdefault(str(it["id"]), it)
    out = []
    for n in range(len(passes) - 1):
        A, B = passes[n], passes[n + 1]
        both = set(A) & set(B)
        ws = [k for k in both if A[k].get("sold") is not None and B[k].get("sold") is not None]
        out.append(dict(from_pass=starts[n], to_pass=starts[n + 1],
                        hours_between=round((datetime.fromisoformat(starts[n + 1]) - datetime.fromisoformat(starts[n])).total_seconds() / 3600, 2),
                        products_in_both=len(both), with_sold=len(ws), sold_up=sum(B[k]["sold"] > A[k]["sold"] for k in ws),
                        units_sold=sum(max(0, B[k]["sold"] - A[k]["sold"]) for k in ws),
                        price_changed=sum(A[k]["price"] != B[k]["price"] for k in both)))
    df = pd.DataFrame(out)
    if len(df):
        df["sold_up_share"] = (df["sold_up"] / df["with_sold"].clip(lower=1)).round(4)
        df["price_changed_share"] = (df["price_changed"] / df["products_in_both"].clip(lower=1)).round(4)
    return df


# ============================================================================
# Cross-site and crawl health
# ============================================================================

def cross_site(raw):
    rows, docs = [], []
    for r in jl(raw / "pairs.jsonl"):
        f, w = (r["fpt"]["offer"] or {}), (r["wss"]["offer"] or {})
        if not (f.get("price") and w.get("low")):
            continue
        p, lo, hi = float(f["price"]), float(w["low"]), float(w["high"] or w["low"])
        row = dict(checked_at=r["ts"], fpt_product=f.get("name"), fpt_price=p, wss_low=lo, wss_high=hi,
                   wss_offer_count=w.get("count"), ratio_to_low=round(p / lo, 4), inside_band=lo <= p <= hi,
                   position="below low" if p < lo else "above high" if p > hi else "inside band")
        rows.append(row)
        docs.append({"@timestamp": r["ts"], "product": f.get("name"), "fpt_price": p, "wss_low": lo, "wss_high": hi,
                     "ratio_to_low": row["ratio_to_low"], "position": row["position"], "alert": yn(not row["inside_band"])})
    return pd.DataFrame(rows), docs


def crawl_docs(raw):
    docs = []
    for job in ("chotot_models", "chotot_stream", "dmx", "tiki"):
        for r in jl(raw / f"{job}.jsonl"):
            docs.append({"@timestamp": r["ts"], "job": job, "status": str(r["status"]), "secs": r["secs"],
                         "items": r["n"], "size_kb": round(r["size"] / 1024, 1),
                         "empty_page": yn(r["status"] == 200 and r["n"] == 0)})
    for c in jl(raw / "chotot_track.jsonl"):
        docs.append({"@timestamp": c["ts"], "job": "chotot_track", "status": str(c["status"]), "secs": c["secs"],
                     "items": int(bool(c["alive"])), "size_kb": round(c["size"] / 1024, 1), "empty_page": "no"})
    for r in jl(raw / "pairs.jsonl"):
        for side in ("fpt", "wss"):
            docs.append({"@timestamp": r["ts"], "job": f"pairs_{side}", "status": str(r[side]["status"]),
                         "secs": r[side]["secs"], "items": int(bool(r[side]["offer"])), "size_kb": None, "empty_page": "no"})
    return docs


# ============================================================================
# Benchmark scoreboard (from the results JSONs)
# ============================================================================

TIKI_CHOSEN = os.environ.get("TIKI_CHOSEN", "isolation_forest")


def scoreboard(res, tiki_summary, tiki_n):
    rows = []

    def add(domain, layer, task, method, metric, value, chosen=False):
        if value is not None:
            rows.append(dict(domain=domain, layer=layer, task=task, method=method, metric=metric,
                             value=round(float(value), 4), chosen=chosen))
    a = json.load(open(res / "chotot_anomaly_bench.json", encoding="utf-8"))
    for m, v in a["synthetic"].items():
        for met in ("auc", "ap", "f1"):
            add("Rao vặt (Chợ Tốt)", "batch", "Giá bất thường theo (model, dung lượng)", m, met, v[met]["mean"], m.startswith(("M1", "M2")))
    for m, v in a["speed"]["latency_us"].items():
        add("Rao vặt (Chợ Tốt)", "speed", "Chấm điểm tin mới", m, "latency_p50_us", v["p50"], m == "rule_mad_z")
    add("Rao vặt (Chợ Tốt)", "speed", "Chấm điểm tin mới", "rule_mad_z", "baseline_coverage", a["speed"]["coverage_accumulated_baseline"], True)
    s = json.load(open(res / "chotot_survival_bench.json", encoding="utf-8"))
    for m, v in s["cox_cv_concordance"].items():
        add("Rao vặt (Chợ Tốt)", "batch", "Thời gian tới khi tin bị gỡ", m, "c_index", v["mean"])
    add("Rao vặt (Chợ Tốt)", "batch", "Thời gian tới khi tin bị gỡ", "kaplan_meier", "removed_listings", s["cohort"]["events"], True)
    r = json.load(open(res / "retail_bench.json", encoding="utf-8"))
    for m, v in r["dmx"]["change_detection"]["methods"].items():
        add("Bán lẻ (ĐMX, Tiki)", "speed", "Phát hiện đổi giá", m, "recall", v["recall"], m.startswith("S1"))
        add("Bán lẻ (ĐMX, Tiki)", "speed", "Phát hiện đổi giá", m, "false_alarm_per_1000_obs", v["false_alarm_per_1000_obs"], m.startswith("S1"))
    for m, v in r["dmx"]["spike_detection"]["methods"].items():
        add("Bán lẻ (ĐMX, Tiki)", "batch", "Giá bất thường theo lịch sử offer", m, "f1", v["f1"]["mean"], m.startswith("B2"))
        add("Bán lẻ (ĐMX, Tiki)", "batch", "Giá bất thường theo lịch sử offer", m, "scorable_share", v["scorable_share"]["mean"], m.startswith("B2"))
    for m, v in tiki_summary.items():
        add("Bán lẻ (ĐMX, Tiki)", "batch", f"Giá bất thường theo danh mục (Tiki, {tiki_n} SP)", m, "auc", v["auc"][0], m == TIKI_CHOSEN)
        add("Bán lẻ (ĐMX, Tiki)", "batch", f"Giá bất thường theo danh mục (Tiki, {tiki_n} SP)", m, "ap", v["ap"][0], m == TIKI_CHOSEN)
    e = json.load(open(res / "entity_bench.json", encoding="utf-8"))
    for key, v in e.items():
        if isinstance(v, dict) and "methods" in v:
            for m, x in v["methods"].items():
                add("So giá chéo", "batch", "Ghép sản phẩm: " + key.replace("fptshop_to_", "FPT → ").replace("__", ", "),
                    m, "f1", x["f1"], m.startswith("block"))
    c = json.load(open(res / "cross_site_bench.json", encoding="utf-8"))
    last = max(k for k in c if k.startswith("pass_"))
    add("So giá chéo", "speed", "So giá với dải giá thị trường", "band_check", "outside_band_share", 1 - c[last]["fpt_inside_band_share"], True)
    sc = json.load(open(res / "scale_bench.json", encoding="utf-8"))
    for m, v in sc["runs"].items():
        if isinstance(v.get("1000000"), (int, float)) and m.startswith("rule"):  # the model timings ran on Cho Tot rows
            add("Chi phí", "batch", "Fit/score 1M dòng (giây)", m, "seconds_1m", v["1000000"], m.startswith("rule"))
    return pd.DataFrame(rows)


# ============================================================================
# Kibana dashboard
# ============================================================================

def kibana_objects(worktree):
    sys.path.insert(0, str(worktree))
    from display.kibana import marketplace_dashboards as kd
    views = [("dv-mlb-listings", "mlbench-chotot-listings", "Chợ Tốt: tin mới + robust z"),
             ("dv-mlb-lifecycle", "mlbench-chotot-lifecycle", "Chợ Tốt: tin bị gỡ"),
             ("dv-mlb-changes", "mlbench-price-changes", "Bán lẻ: sự kiện đổi giá"),
             ("dv-mlb-band", "mlbench-band-check", "So giá chéo: kiểm tra dải giá"),
             ("dv-mlb-crawl", "mlbench-crawl", "Crawl: từng request")]
    objs = [kd._data_view_object({"id": i, "title": t, "name": n, "timeFieldName": "@timestamp"}) for i, t, n in views]
    objs.append(kd._saved_search("mlb-flagged-listings", title="Chợ Tốt: tin bị gắn cờ (|robust z| > 3,5)",
                                 description="Tin mới có giá lệch mạnh so với mốc (model, dung lượng) tích luỹ từ các tin crawl trước đó.",
                                 data_view="dv-mlb-listings", sort_field="robust_z",
                                 columns=["subject", "price", "baseline_median", "price_ratio", "robust_z", "baseline_level", "dealer", "defect_in_title"]))
    objs.append(kd._saved_search("mlb-price-change-list", title="Bán lẻ: sự kiện đổi giá",
                                 description="Mỗi lần giá khác giá quan sát trước. rule_detected luôn là yes; cột CUSUM/Page-Hinkley cho thấy hai thuật toán đó có bắt được hay không.",
                                 data_view="dv-mlb-changes", sort_field="@timestamp",
                                 columns=["source", "name", "old_price", "new_price", "pct_change", "cusum_detected", "page_hinkley_detected"]))
    P = kd._Panels()
    g = kd._grid
    P.add(kd._lens_panel("p1", title="Chợ Tốt: tin mới theo thời gian, theo danh mục",
                         description="Mỗi tin lần đầu xuất hiện ở trang 1 của danh mục (poll mỗi 3 phút).",
                         data_view="dv-mlb-listings", visualization_type="lnsXY",
                         columns={"x": kd._date_histogram("@timestamp", label="thời gian"),
                                  "s": kd._terms("category", size=3, label="danh mục"), "y": kd._count("Tin mới")},
                         visualization=kd._xy("layer-p1", x="x", ys=["y"], split="s"), grid=g(0, 0, 24, 13, "p1")))
    P.add(kd._lens_panel("p2", title="Chợ Tốt: độ phủ mốc chuẩn cho tin mới",
                         description="model+capacity / model: đủ ≥ 8 tin trước đó để tính median, MAD. none: chưa có mốc chuẩn.",
                         data_view="dv-mlb-listings", visualization_type="lnsPie", query='category: "mua-ban-dien-thoai"',
                         columns={"s": kd._terms("baseline_level", size=3, order_by="m", label="mốc chuẩn"), "m": kd._count("Tin")},
                         visualization=kd._pie("layer-p2", group="s", metric="m"), grid=g(24, 0, 24, 13, "p2")))
    P.add(kd._lens_panel("p3", title="Chợ Tốt: tin bị gắn cờ theo thời gian",
                         description="|robust z| > 3,5 so với mốc chuẩn tích luỹ.",
                         data_view="dv-mlb-listings", visualization_type="lnsXY", query='flagged: "yes"',
                         columns={"x": kd._date_histogram("@timestamp", label="thời gian"), "y": kd._count("Tin bị gắn cờ")},
                         visualization=kd._xy("layer-p3", x="x", ys=["y"]), grid=g(0, 13, 24, 13, "p3")))
    P.add(kd._lens_panel("p4", title="Chợ Tốt: tin bị gỡ (HTTP 410) theo thời gian",
                         description="Thời điểm phát hiện ở lượt kiểm tra; tin thực sự bị gỡ giữa lượt trước và lượt này.",
                         data_view="dv-mlb-lifecycle", visualization_type="lnsXY",
                         columns={"x": kd._date_histogram("@timestamp", label="thời gian"),
                                  "s": kd._terms("category", size=3, label="danh mục"), "y": kd._count("Tin bị gỡ")},
                         visualization=kd._xy("layer-p4", x="x", ys=["y"], split="s"), grid=g(24, 13, 24, 13, "p4")))
    P.add(kd._search_panel("p5", search_id="mlb-flagged-listings", title="Chợ Tốt: tin bị gắn cờ", grid=g(0, 26, 48, 12, "p5")))
    P.add(kd._lens_panel("p6", title="Bán lẻ: sự kiện đổi giá theo nguồn",
                         description="Luật giá ≠ giá trước (recall 1,00 trên dữ liệu đo).",
                         data_view="dv-mlb-changes", visualization_type="lnsXY",
                         columns={"x": kd._date_histogram("@timestamp", label="thời gian"),
                                  "s": kd._terms("source", size=2, label="nguồn"), "y": kd._count("Lần đổi giá")},
                         visualization=kd._xy("layer-p6", x="x", ys=["y"], split="s"), grid=g(0, 38, 24, 13, "p6")))
    P.add(kd._lens_panel("p7", title="Bán lẻ: CUSUM có bắt được lần đổi giá không",
                         description="Cùng các sự kiện đổi giá; yes nếu CUSUM báo trong ≤ 3 quan sát.",
                         data_view="dv-mlb-changes", visualization_type="lnsPie",
                         columns={"s": kd._terms("cusum_detected", size=2, order_by="m", label="CUSUM"), "m": kd._count("Sự kiện")},
                         visualization=kd._pie("layer-p7", group="s", metric="m"), grid=g(24, 38, 24, 13, "p7")))
    P.add(kd._search_panel("p8", search_id="mlb-price-change-list", title="Bán lẻ: danh sách đổi giá", grid=g(0, 51, 48, 12, "p8")))
    P.add(kd._lens_panel("p9", title="So giá chéo: vị trí giá FPT so với dải Websosanh",
                         description="Mỗi lần kiểm tra một cặp laptop đã ghép theo mã part number.",
                         data_view="dv-mlb-band", visualization_type="lnsPie",
                         columns={"s": kd._terms("position", size=3, order_by="m", label="vị trí"), "m": kd._count("Lần kiểm tra")},
                         visualization=kd._pie("layer-p9", group="s", metric="m"), grid=g(0, 63, 24, 13, "p9")))
    P.add(kd._lens_panel("p10", title="Crawl: request theo HTTP status",
                         description="Tất cả request của mọi job, gồm 403 Cloudflare và 410 tin đã gỡ.",
                         data_view="dv-mlb-crawl", visualization_type="lnsXY",
                         columns={"x": kd._date_histogram("@timestamp", label="thời gian"),
                                  "s": kd._terms("status", size=6, label="status"), "y": kd._count("Request")},
                         visualization=kd._xy("layer-p10", x="x", ys=["y"], split="s"), grid=g(24, 63, 24, 13, "p10")))
    P.add(kd._lens_panel("p11", title="Crawl: thời gian phản hồi theo job",
                         description="Trung vị số giây mỗi request.",
                         data_view="dv-mlb-crawl", visualization_type="lnsXY",
                         columns={"x": kd._date_histogram("@timestamp", label="thời gian"),
                                  "s": kd._terms("job", size=7, label="job"),
                                  "y": kd._metric("median", "secs", "Trung vị giây/request")},
                         visualization=kd._xy("layer-p11", x="x", ys=["y"], split="s", series_type="line"), grid=g(0, 76, 48, 13, "p11")))
    objs.append(kd._dashboard("mlbench-speed-view", title="ML bench — speed layer (sự kiện)",
                              description="Kết quả chấm điểm từng sự kiện: tin Chợ Tốt mới (robust z, chỉ thống kê), đổi giá bán lẻ, kiểm tra dải giá chéo, sức khoẻ crawl.",
                              panels=P.panels, references=P.references, time_from="now-7d"))
    nd = "\n".join(json.dumps(kd._stamped(o), ensure_ascii=False) for o in objs) + "\n"
    r = requests.post(f"{KIBANA}/api/saved_objects/_import", params={"overwrite": "true"},
                      headers={"kbn-xsrf": "true"}, files={"file": ("mlbench.ndjson", nd.encode("utf-8"), "application/ndjson")}, timeout=60)
    r.raise_for_status()
    j = r.json()
    if not j.get("success"):
        raise RuntimeError(json.dumps(j)[:1500])
    return j["successCount"]


# ============================================================================
# Superset dashboard
# ============================================================================

class Superset:
    def __init__(self):
        self.s = requests.Session()
        t = self.s.post(f"{SUPERSET}/api/v1/security/login", json={"username": "admin", "password": "admin", "provider": "db", "refresh": True}, timeout=30)
        t.raise_for_status()
        self.tok = t.json()["access_token"]
        c = self.s.get(f"{SUPERSET}/api/v1/security/csrf_token/", headers=self.h(), timeout=30)
        c.raise_for_status()
        self.csrf = c.json()["result"]

    def h(self, write=False):
        h = {"Authorization": f"Bearer {self.tok}"}
        if write:
            h.update({"X-CSRFToken": self.csrf, "Referer": SUPERSET, "Content-Type": "application/json"})
        return h

    def get(self, path):
        r = self.s.get(f"{SUPERSET}{path}", headers=self.h(), timeout=60)
        r.raise_for_status()
        return r.json()

    def post(self, path, body):
        r = self.s.post(f"{SUPERSET}{path}", headers=self.h(True), json=body, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"POST {path}: {r.status_code} {r.text[:800]}")
        return r.json()

    def put(self, path, body):
        r = self.s.put(f"{SUPERSET}{path}", headers=self.h(True), json=body, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"PUT {path}: {r.status_code} {r.text[:800]}")
        return r.json()

    def delete(self, path):
        self.s.delete(f"{SUPERSET}{path}", headers=self.h(True), timeout=60)

    def database(self):
        for d in self.get("/api/v1/database/?q=(page_size:100)")["result"]:
            if d["database_name"] == "mlbench_dw":
                return d["id"]
        return self.post("/api/v1/database/", {"database_name": "mlbench_dw", "sqlalchemy_uri": PG_FROM_SUPERSET, "expose_in_sqllab": True})["id"]

    def dataset(self, db, table):
        for d in self.get("/api/v1/dataset/?q=(page_size:500)")["result"]:
            if d["schema"] == SCHEMA and d["table_name"] == table:
                self.s.put(f"{SUPERSET}/api/v1/dataset/{d['id']}/refresh", headers=self.h(True), timeout=60)
                return d["id"]
        return self.post("/api/v1/dataset/", {"database": db, "schema": SCHEMA, "table_name": table})["id"]


def M(col, agg="MAX", label=None):
    return {"expressionType": "SIMPLE", "column": {"column_name": col}, "aggregate": agg, "label": label or f"{agg}({col})"}


def where(sql):
    return [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": sql}]


def query_context(ds, p):
    """A plain query for the chart-data API: the same columns, metrics and WHERE the chart uses."""
    where_sql = " AND ".join(f["sqlExpression"] for f in p.get("adhoc_filters", []))
    if p.get("query_mode") == "raw":
        q = {"columns": p["all_columns"], "metrics": [], "orderby": [json.loads(o) for o in p.get("order_by_cols", [])],
             "row_limit": p["row_limit"]}
    else:
        q = {"columns": [p["x_axis"], *p.get("groupby", [])], "metrics": p["metrics"], "orderby": [], "row_limit": p["row_limit"]}
    q.update({"filters": [], "extras": {"where": where_sql} if where_sql else {}})
    return {"datasource": {"id": ds, "type": "table"}, "force": False, "queries": [q], "form_data": p,
            "result_format": "json", "result_type": "full"}


def superset_dashboard(tables):
    sp = Superset()
    db = sp.database()
    ids = {t: sp.dataset(db, t) for t in tables}
    charts = [
        ("Bảng xếp hạng phương pháp (giá trị đo thật)", "method_metrics", "table",
         {"query_mode": "raw", "all_columns": ["domain", "layer", "task", "method", "metric", "value", "chosen"],
          "order_by_cols": [json.dumps(["domain", True]), json.dumps(["task", True]), json.dumps(["value", False])], "row_limit": 500}, 12, 60),
        ("Chợ Tốt: AUC phát hiện giá bất thường theo phương pháp", "method_metrics", "echarts_timeseries_bar",
         {"x_axis": "method", "metrics": [M("value", "MAX", "AUC")], "groupby": [], "adhoc_filters": where("task = 'Giá bất thường theo (model, dung lượng)' AND metric = 'auc'"),
          "row_limit": 50, "x_axis_sort_asc": True, "orientation": "vertical"}, 6, 50),
        ("Tiki: AUC luật vs Isolation Forest vs ECOD", "method_metrics", "echarts_timeseries_bar",
         {"x_axis": "method", "metrics": [M("value", "MAX", "AUC")], "groupby": [], "adhoc_filters": where("task LIKE 'Giá bất thường theo danh mục (Tiki%' AND metric = 'auc'"),
          "row_limit": 50, "orientation": "vertical"}, 6, 50),
        ("Bán lẻ: tỷ lệ quan sát chấm được theo luật", "retail_rule_coverage", "echarts_timeseries_bar",
         {"x_axis": "method", "metrics": [M("scorable_share", "MAX", "Tỷ lệ chấm được")], "groupby": ["source"], "adhoc_filters": [],
          "row_limit": 50, "orientation": "vertical"}, 6, 50),
        ("Chợ Tốt: đường Kaplan-Meier (tin còn sống theo giờ)", "survival_km", "echarts_timeseries_line",
         {"x_axis": "t_hours", "metrics": [M("survival", "MAX", "S(t)")], "groupby": ["category"], "adhoc_filters": [],
          "row_limit": 5000, "x_axis_sort_asc": True}, 6, 50),
        ("Chợ Tốt: mốc giá theo (model, dung lượng)", "chotot_baseline", "table",
         {"query_mode": "raw", "all_columns": ["model", "capacity", "listings", "median_price", "p25", "p75", "mad_log"],
          "order_by_cols": [json.dumps(["listings", False])], "row_limit": 300}, 6, 50),
        ("Chợ Tốt: tin bị gắn cờ", "chotot_flagged", "table",
         {"query_mode": "raw", "all_columns": ["first_seen", "subject", "price", "baseline_median", "price_ratio", "robust_z", "baseline_level", "defect_in_title"],
          "order_by_cols": [json.dumps(["first_seen", False])], "row_limit": 300}, 6, 50),
        ("Bán lẻ: offer có giá lệch lịch sử", "retail_offer_anomaly", "table",
         {"query_mode": "raw", "all_columns": ["source", "observed_at", "name", "price", "rolling_median", "pct_deviation", "kind", "current_rule_verdict"],
          "order_by_cols": [json.dumps(["observed_at", False])], "row_limit": 300}, 12, 50),
        ("Tiki: sản phẩm điểm bất thường cao nhất (luật và ML)", "tiki_offer_scores", "table",
         {"query_mode": "raw", "all_columns": ["name", "leaf", "price", "robust_z", "iforest_score", "ecod_score", "reviews", "sold"],
          "order_by_cols": [json.dumps(["robust_z", False])], "row_limit": 300}, 6, 50),
        ("Tiki: tín hiệu nhu cầu giữa các lượt quét", "tiki_demand", "table",
         {"query_mode": "raw", "all_columns": ["from_pass", "to_pass", "hours_between", "products_in_both", "sold_up", "sold_up_share", "units_sold", "price_changed_share"],
          "order_by_cols": [json.dumps(["from_pass", True])], "row_limit": 100}, 6, 50),
        ("So giá chéo: giá FPT / lowPrice Websosanh", "cross_site_gap", "table",
         {"query_mode": "raw", "all_columns": ["checked_at", "fpt_product", "fpt_price", "wss_low", "wss_high", "ratio_to_low", "position"],
          "order_by_cols": [json.dumps(["ratio_to_low", False])], "row_limit": 500}, 12, 50),
    ]
    old = sp.get("/api/v1/chart/?q=(page_size:500)")["result"]
    names = {c[0] for c in charts}
    for c in old:
        if c["slice_name"] in names:
            sp.delete(f"/api/v1/chart/{c['id']}")
    made, problems = [], []
    for name, table, viz, params, width, height in charts:
        p = dict(params, datasource=f"{ids[table]}__table", viz_type=viz)
        cid = sp.post("/api/v1/chart/", {"slice_name": name, "viz_type": viz, "datasource_type": "table",
                                         "datasource_id": ids[table], "params": json.dumps(p),
                                         "query_context": json.dumps(query_context(ids[table], p))})["id"]
        made.append((cid, width, height))
    slug = "mlbench-batch-view"
    dash = next((d["id"] for d in sp.get("/api/v1/dashboard/?q=(page_size:200)")["result"] if d.get("slug") == slug), None)
    if dash is None:
        dash = sp.post("/api/v1/dashboard/", {"dashboard_title": "ML bench — batch layer (bảng, mô hình, xếp hạng)", "slug": slug, "published": True})["id"]
    for cid, _, _ in made:
        sp.put(f"/api/v1/chart/{cid}", {"dashboards": [dash]})
    pos = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"], "meta": {}},
           "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": [], "parents": ["ROOT_ID"], "meta": {}}}
    row, used, n = None, 12, 0
    for cid, w, h in made:
        if used + w > 12:
            n += 1
            row = f"ROW-{n}"
            pos[row] = {"id": row, "type": "ROW", "children": [], "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            pos["GRID_ID"]["children"].append(row)
            used = 0
        pos[row]["children"].append(f"CHART-{cid}")
        pos[f"CHART-{cid}"] = {"id": f"CHART-{cid}", "type": "CHART", "children": [], "parents": ["ROOT_ID", "GRID_ID", row],
                               "meta": {"chartId": cid, "width": w, "height": h}}
        used += w
    sp.put(f"/api/v1/dashboard/{dash}", {"slug": slug, "published": True, "position_json": json.dumps(pos)})
    # render check: ask Superset for every chart's data, as the dashboard would
    for cid, _, _ in made:
        r = sp.s.get(f"{SUPERSET}/api/v1/chart/{cid}/data/", headers=sp.h(), timeout=120)
        if r.status_code >= 400:
            problems.append((cid, r.status_code, r.text[:300]))
        else:
            rows = sum(len(q.get("data") or []) for q in r.json().get("result", []))
            if rows == 0:
                problems.append((cid, "no rows", ""))
    return f"{SUPERSET}/superset/dashboard/{slug}/", len(made), problems


# ============================================================================

def main():
    raw, res, worktree = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    out = {}
    listings, baseline = chotot_listings(raw)
    lifecycle, km = chotot_lifecycle(raw, listings)
    dmx, tiki = retail_series(raw, "dmx"), retail_series(raw, "tiki")
    changes = price_change_docs(dmx, "dmx") + price_change_docs(tiki, "tiki")
    an_rows, cov = [], []
    for S, src in ((dmx, "dmx"), (tiki, "tiki")):
        r_, c_ = retail_offer_anomaly(S, src)
        an_rows += r_
        cov += c_
    top, tiki_summary, tiki_n, tiki_inj = tiki_ml(tiki)
    demand = tiki_demand(raw)
    gap, band = cross_site(raw)
    crawl = crawl_docs(raw)

    out["es"] = {
        "mlbench-chotot-listings": es_load("mlbench-chotot-listings", {
            "@timestamp": D, "list_id": I, "category": K, "subject": TXT, "model": K, "capacity": K, "price": F, "dealer": K,
            "baseline_level": K, "baseline_n": I, "baseline_median": F, "price_ratio": F, "robust_z": F, "flagged": K,
            "defect_in_title": K, "day": K}, listings),
        "mlbench-chotot-lifecycle": es_load("mlbench-chotot-lifecycle", {
            "@timestamp": D, "list_id": I, "category": K, "price": F, "hours_since_posted": F, "last_alive_at": D, "status": K}, lifecycle),
        "mlbench-price-changes": es_load("mlbench-price-changes", {
            "@timestamp": D, "source": K, "offer_id": K, "name": TXT, "category": K, "old_price": F, "new_price": F,
            "pct_change": F, "direction": K, "rule_detected": K, "cusum_detected": K, "page_hinkley_detected": K,
            "minutes_since_prev": F}, changes),
        "mlbench-band-check": es_load("mlbench-band-check", {
            "@timestamp": D, "product": TXT, "fpt_price": F, "wss_low": F, "wss_high": F, "ratio_to_low": F, "position": K, "alert": K}, band),
        "mlbench-crawl": es_load("mlbench-crawl", {
            "@timestamp": D, "job": K, "status": K, "secs": F, "items": I, "size_kb": F, "empty_page": K}, crawl),
    }
    out["kibana_objects"] = kibana_objects(worktree)

    flagged = pd.DataFrame([d for d in listings if d["flagged"] == "yes"]).rename(columns={"@timestamp": "first_seen"})
    tables = {
        "method_metrics": scoreboard(res, tiki_summary, tiki_n),
        "chotot_baseline": baseline,
        "chotot_flagged": flagged,
        "survival_km": km,
        "retail_offer_anomaly": pd.DataFrame(an_rows),
        "retail_rule_coverage": pd.DataFrame(cov),
        "tiki_offer_scores": top,
        "tiki_demand": demand,
        "cross_site_gap": gap,
    }
    eng = create_engine(PG)
    with eng.begin() as c:
        c.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
    for name, df in tables.items():
        if "first_seen" in df.columns:
            df["first_seen"] = pd.to_datetime(df["first_seen"])
        for col in ("observed_at", "checked_at"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        df.to_sql(name, eng, schema=SCHEMA, if_exists="replace", index=False)
    out["postgres_rows"] = {k: len(v) for k, v in tables.items()}
    out["tiki_ml"] = dict(products=tiki_n, injected_per_run=tiki_inj, metrics=tiki_summary)
    url, n, problems = superset_dashboard(list(tables))
    out["superset"] = dict(url=url, charts=n, problems=problems)
    out["kibana_url"] = f"{KIBANA}/app/dashboards#/view/mlbench-speed-view"
    json.dump(out, open(res / "publish.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=str)
    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))


if __name__ == "__main__":
    main()

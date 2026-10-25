"""Price anomaly benchmark on Cho Tot phone listings (classifieds domain).

Batch: fit on the model-page snapshot (one listing per list_id), group key = (model, capacity).
  M1 IQR fence per group, M2 robust z on log price (MAD) per group, M3 IsolationForest, M4 LOF, M5 ECOD
  (M3-M5 on listing features: log price / group median, condition, dealer flag, images, seller history, title flags).
Eval A, synthetic: 3% of listings in groups with >= 8 listings get price x {0.2 .. 5}; ROC-AUC, AP, P/R/F1 of each
  method's own decision rule, 10 seeds.
Eval B, real: each method's top 25 on the untouched data go to a manual audit file (labelled by reading the title).
Speed: per-group baselines from the batch fit are looked up for every streamed new listing; per-event latency
  and baseline coverage.

usage: python chotot_anomaly_bench.py <raw_dir> <results_dir>
"""
import json, math, os, re, statistics, sys, time
from pathlib import Path

import numpy as np
import pandas as pd
from pyod.models.ecod import ECOD
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.neighbors import LocalOutlierFactor

FLAGS = {
    "defect": r"v[ỡơ]|h[ỏo]ng|l[ỗo]i|m[àa]n v[àa]ng|s[ọo]c|[áa]m m[àa]n|x[áa]c|linh ki[ệe]n|b[ểe]|h[ưu] |icloud|kh[óo]a|treo logo|main",
    "installment": r"g[óo]p|tr[ảa] tr[ướu][ớo]c|tr[ảa] g[óo]p",
    "buying": r"thu mua|c[ầa]n mua|mua l[ạa]i|thu c[ũu]",
    "like_new": r"99|98|like ?new|nh[ưu] m[ớo]i|keng|zin",
}
MIN_GROUP = 8
# Cho Tot's robots.txt carries Content-Signal ai-train=no: STATS_ONLY=1 skips the learned models (M3-M5)
STATS_ONLY = os.environ.get("STATS_ONLY") == "1"


def load(raw):
    rows, seen = [], set()
    first_pass_end = None
    for line in open(raw / "chotot_models.jsonl", encoding="utf-8"):
        r = json.loads(line)
        for it in r["items"]:
            if it["list_id"] in seen:
                continue
            seen.add(it["list_id"])
            rows.append(dict(it, ts=r["ts"]))
    stream, sseen = [], set(seen)
    for line in open(raw / "chotot_stream.jsonl", encoding="utf-8"):
        r = json.loads(line)
        if "dien-thoai" not in r["url"]:
            continue
        for it in r["items"]:
            if it["list_id"] not in sseen:
                sseen.add(it["list_id"])
                stream.append(dict(it, ts=r["ts"]))
    return rows, stream


def frame(rows):
    df = pd.DataFrame(rows)
    df["model"] = df["params"].map(lambda p: (p or {}).get("mobile_model"))
    df["cap"] = df["params"].map(lambda p: (p or {}).get("mobile_capacity"))
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["subject"] = df["subject"].fillna("").str.lower()
    for k, pat in FLAGS.items():
        df["f_" + k] = df["subject"].str.contains(pat, regex=True).astype(int)
    df["dealer"] = df["company_ad"].map(lambda v: int(bool(v)))
    df["cond_used"] = (df["elt_condition"] == 2).astype(int)
    df["imgs"] = pd.to_numeric(df["number_of_images"], errors="coerce").fillna(0)
    df["log_sold_ads"] = np.log1p(pd.to_numeric(df["sold_ads"], errors="coerce").fillna(0))
    df["key"] = df["model"].astype(str) + " | " + df["cap"].astype(str)
    return df


def assign_keys(df, ref):
    """(model, capacity) when that group has >= MIN_GROUP listings in ref, else the whole model, else no baseline."""
    full = ref["model"].astype(str) + " | " + ref["cap"].astype(str)
    n_full, n_model = full.value_counts(), ref["model"].astype(str).value_counts()
    k_full = df["model"].astype(str) + " | " + df["cap"].astype(str)
    k_model = df["model"].astype(str) + " | *"
    use_full = k_full.map(n_full).fillna(0) >= MIN_GROUP
    use_model = df["model"].astype(str).map(n_model).fillna(0) >= MIN_GROUP
    df["key"] = np.where(use_full, k_full, np.where(use_model, k_model, "none"))
    df["key_level"] = np.where(use_full, "model+capacity", np.where(use_model, "model", "none"))
    return df


def group_stats(df):
    out = {}
    model = df["model"].astype(str)
    for k in df.loc[df["key"] != "none", "key"].unique():
        # a model-level key ("A | *") takes its baseline from every listing of that model
        g = df[model == k[:-4]] if k.endswith(" | *") else df[df["key"] == k]
        lp = np.log(g["price"].values)
        if len(lp) < MIN_GROUP:
            continue
        med = float(np.median(lp))
        mad = float(np.median(np.abs(lp - med)))
        q1, q3 = np.percentile(g["price"].values, [25, 75])
        out[k] = dict(n=len(lp), med_log=med, mad_log=mad, q1=float(q1), q3=float(q3), median=float(np.exp(med)))
    return out


def score_rules(df, stats):
    z, iqr = np.full(len(df), np.nan), np.full(len(df), np.nan)
    for n, (k, p) in enumerate(zip(df["key"].values, df["price"].values)):
        s = stats.get(k)
        if not s:
            continue
        z[n] = abs(math.log(p) - s["med_log"]) / (1.4826 * s["mad_log"]) if s["mad_log"] > 0 else 0.0
        r = s["q3"] - s["q1"]
        iqr[n] = max(s["q1"] - p, p - s["q3"], 0) / r if r > 0 else 0.0
    return z, iqr


FEATS = ["log_ratio", "cond_used", "dealer", "imgs", "log_sold_ads", "f_defect", "f_installment", "f_buying", "f_like_new"]


def add_ratio(df, stats):
    med = df["key"].map(lambda k: stats[k]["med_log"] if k in stats else np.nan)
    df["log_ratio"] = np.log(df["price"]) - med
    return df


def evaluate(y, score, flag):
    tp = int(((flag == 1) & (y == 1)).sum()); fp = int(((flag == 1) & (y == 0)).sum()); fn = int(((flag == 0) & (y == 1)).sum())
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    return dict(auc=roc_auc_score(y, score), ap=average_precision_score(y, score), precision=p, recall=r,
                f1=2 * p * r / (p + r) if p + r else 0.0)


def run_methods(df):
    """Returns {method: (score, flag, fit_seconds)} on rows that belong to a group with a baseline."""
    stats = group_stats(df)
    df = add_ratio(df, stats)
    m = df["log_ratio"].notna().values
    d = df[m].reset_index(drop=True)
    out = {}
    t0 = time.perf_counter()
    z, iqr = score_rules(d, stats)
    t_rule = time.perf_counter() - t0
    out["M1_iqr_fence"] = (iqr, (iqr > 1.5).astype(int), t_rule)
    out["M2_mad_robust_z"] = (z, (z > 3.5).astype(int), t_rule)
    if STATS_ONLY:
        return d, stats, out, None, None
    X = d[FEATS].values.astype(float)
    t0 = time.perf_counter()
    ifo = IsolationForest(n_estimators=200, contamination="auto", random_state=0).fit(X)
    s = -ifo.score_samples(X)
    out["M3_isolation_forest"] = (s, (ifo.predict(X) == -1).astype(int), time.perf_counter() - t0)
    t0 = time.perf_counter()
    lof = LocalOutlierFactor(n_neighbors=20)
    lab = lof.fit_predict(X)
    out["M4_lof"] = (-lof.negative_outlier_factor_, (lab == -1).astype(int), time.perf_counter() - t0)
    t0 = time.perf_counter()
    ec = ECOD(contamination=0.05).fit(X)
    out["M5_ecod"] = (ec.decision_scores_, ec.labels_.astype(int), time.perf_counter() - t0)
    return d, stats, out, ifo, ec


def main():
    raw, res = Path(sys.argv[1]), Path(sys.argv[2])
    rows, stream = load(raw)
    base = frame(rows)
    invalid = base[(base["price"].isna()) | (base["price"] <= 1000)]
    base = base[base["price"] > 1000].reset_index(drop=True)
    base = assign_keys(base, base)
    report = dict(listings=len(rows), invalid_price_le_1000=len(invalid), usable=len(base),
                  groups_ge_8=0, rows_in_groups_ge_8=0, synthetic={}, timings={}, speed={})

    # ---- Eval A: synthetic injection --------------------------------------------------------
    factors = [0.2, 0.35, 0.5, 2.0, 3.0, 5.0]
    agg = {}
    for seed in range(10):
        rng = np.random.default_rng(seed)
        df = base.copy()
        st0 = group_stats(df)
        elig = np.where(df["key"].isin(list(st0)).values)[0]
        inj = rng.choice(elig, size=int(0.03 * len(elig)), replace=False)
        df.loc[inj, "price"] = df.loc[inj, "price"] * rng.choice(factors, size=len(inj))
        df["y"] = 0
        df.loc[inj, "y"] = 1
        d, stats, out, _, _ = run_methods(df)
        for name, (s, flag, secs) in out.items():
            e = evaluate(d["y"].values, s, flag) | dict(fit_seconds=secs)
            for k, v in e.items():
                agg.setdefault(name, {}).setdefault(k, []).append(v)
        if seed == 0:
            report["groups_ge_8"] = len(stats)
            report["rows_in_groups_ge_8"] = len(d)
            report["injected_per_run"] = int(d["y"].sum())
    report["synthetic"] = {n: {k: dict(mean=round(float(np.mean(v)), 4), sd=round(float(np.std(v)), 4)) for k, v in m.items()}
                           for n, m in agg.items()}

    # ---- Eval B: real top-25 per method for manual audit ------------------------------------
    d, stats, out, ifo, ec = run_methods(base.copy())
    audit = {}
    for name, (s, flag, _) in out.items():
        report.setdefault("real_flag_rate", {})[name] = round(float(flag.mean()), 4)
        for n in np.argsort(-s)[:25]:
            r = d.iloc[n]
            a = audit.setdefault(int(r["list_id"]), dict(list_id=int(r["list_id"]), subject=r["subject"][:90], price=int(r["price"]),
                                                       group=r["key"], group_median=int(stats[r["key"]]["median"]),
                                                       dealer=int(r["dealer"]), cond_used=int(r["cond_used"]), methods=[]))
            a["methods"].append(name)
    json.dump(list(audit.values()), open(res / "chotot_audit_candidates.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    # ---- Speed: score streamed new listings against batch baselines -------------------------
    sdf = frame(stream)
    sdf = sdf[sdf["price"] > 1000].reset_index(drop=True)
    sdf = assign_keys(sdf, base)
    covered = sdf["key"].isin(list(stats)).values
    redis_like = {k: (v["med_log"], v["mad_log"], v["q1"], v["q3"]) for k, v in stats.items()}  # HGET per key
    lat = {"rule_mad_z": [], "rule_iqr": [], "isolation_forest_1row": [], "ecod_1row": []}
    sdf = add_ratio(sdf, stats)
    flags = []
    for n in range(len(sdf)):
        k, p = sdf.at[n, "key"], sdf.at[n, "price"]
        t0 = time.perf_counter_ns()
        b = redis_like.get(k)
        zz = abs(math.log(p) - b[0]) / (1.4826 * b[1]) if b and b[1] > 0 else None
        lat["rule_mad_z"].append(time.perf_counter_ns() - t0)
        t0 = time.perf_counter_ns()
        q = (max(b[2] - p, p - b[3], 0) / (b[3] - b[2])) if b and b[3] > b[2] else None
        lat["rule_iqr"].append(time.perf_counter_ns() - t0)
        if b and ifo is not None:
            x = sdf.loc[[n], FEATS].values.astype(float)
            t0 = time.perf_counter_ns(); ifo.score_samples(x); lat["isolation_forest_1row"].append(time.perf_counter_ns() - t0)
            t0 = time.perf_counter_ns(); ec.decision_function(x); lat["ecod_1row"].append(time.perf_counter_ns() - t0)
        if b:
            flags.append(dict(list_id=int(sdf.at[n, "list_id"]), subject=sdf.at[n, "subject"][:80], price=int(p), group=k,
                              z=round(zz, 2) if zz is not None else None, flagged=bool(zz is not None and zz > 3.5)))
    report["baseline_level_share"] = base["key_level"].value_counts(normalize=True).round(3).to_dict()
    report["speed"] = dict(new_listings=len(sdf), with_baseline=int(covered.sum()), coverage=round(float(covered.mean()), 3),
                           coverage_by_level=sdf["key_level"].value_counts(normalize=True).round(3).to_dict(),
                           flagged=sum(f["flagged"] for f in flags),
                           latency_us={k: dict(p50=round(np.percentile(v, 50) / 1000, 1), p99=round(np.percentile(v, 99) / 1000, 1))
                                       for k, v in lat.items() if v},
                           flagged_examples=[f for f in flags if f["flagged"]][:15])
    # Lambda-style baseline: every listing crawled before this one (model pages + earlier stream listings)
    seen = frame(rows + stream)
    seen = seen[seen["price"] > 1000]
    seen_ts = seen["ts"].values
    order = sdf.sort_values("ts").index
    n_full, n_model, cov = {}, {}, []
    hist = seen.sort_values("ts")
    j = 0
    hist_rows = list(zip(hist["ts"].values, hist["model"].astype(str).values, hist["cap"].astype(str).values, hist["list_id"].values))
    for n in order:
        t = sdf.at[n, "ts"]
        while j < len(hist_rows) and hist_rows[j][0] < t:
            _, m_, c_, _ = hist_rows[j]
            n_full[(m_, c_)] = n_full.get((m_, c_), 0) + 1
            n_model[m_] = n_model.get(m_, 0) + 1
            j += 1
        m_, c_ = str(sdf.at[n, "model"]), str(sdf.at[n, "cap"])
        cov.append("model+capacity" if n_full.get((m_, c_), 0) >= MIN_GROUP else "model" if n_model.get(m_, 0) >= MIN_GROUP else "none")
    s_cov = pd.Series(cov)
    report["speed"]["coverage_accumulated_baseline"] = round(float((s_cov != "none").mean()), 3)
    report["speed"]["coverage_accumulated_by_level"] = s_cov.value_counts(normalize=True).round(3).to_dict()
    # coverage of the last third of the stream, once the accumulated baseline has grown
    tail = s_cov.iloc[2 * len(s_cov) // 3:]
    report["speed"]["coverage_accumulated_last_third"] = round(float((tail != "none").mean()), 3)
    json.dump(report, open(res / "chotot_anomaly_bench.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps({k: v for k, v in report.items() if k not in ("speed",)}, ensure_ascii=False, indent=1)[:5000])
    print(json.dumps({k: v for k, v in report["speed"].items() if k != "flagged_examples"}, ensure_ascii=False))


if __name__ == "__main__":
    main()

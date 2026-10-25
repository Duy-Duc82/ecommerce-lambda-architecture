"""Time-to-removal (survival) benchmark on Cho Tot listings first seen in the new-listing stream.

Event: the detail page returns HTTP 410, or the listing's status is no longer "active".
Start: list_time from the detail page (ms), else the first time the stream saw it.
Censored: still alive at the last check. Challenged pages (403) are skipped, never retried around.
Models: Kaplan-Meier (descriptive), Cox PH (lifelines, penalizer 0.1), LightGBM "removed within H h" classifier (H = 3 while follow-up is < 8 h, else 6).
Scored by 5-fold concordance index (Cox) and ROC-AUC (classifier); baselines: random (0.5), price-ratio-only Cox.
Also: stream page-1 saturation (share of polls where all 20 listings were new), arrivals per hour.

usage: python chotot_survival_bench.py <raw_dir> <results_dir>
"""
import json, os, re, sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from lifelines import CoxPHFitter, KaplanMeierFitter
from lifelines.utils import concordance_index
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import KFold

sys.path.insert(0, str(Path(__file__).parent))
from chotot_anomaly_bench import FLAGS  # noqa: E402


def ts(s):
    return datetime.fromisoformat(s).timestamp()


def load(raw):
    first, cat, polls = {}, {}, defaultdict(list)
    prev = {}
    for line in open(raw / "chotot_stream.jsonl", encoding="utf-8"):
        r = json.loads(line)
        ids = [i["list_id"] for i in r["items"]]
        new = [i for i in ids if i not in first]
        if r["url"] in prev:
            polls[r["url"]].append(dict(ts=r["ts"], new=len(new), n=len(ids)))
        prev[r["url"]] = ids
        for it in r["items"]:
            if it["list_id"] not in first:
                first[it["list_id"]] = dict(it, first_seen=ts(r["ts"]), cat=r["url"].rsplit("/", 1)[-1])
    checks = defaultdict(list)
    for line in open(raw / "chotot_track.jsonl", encoding="utf-8"):
        c = json.loads(line)
        checks[c["list_id"]].append(c)
    return first, checks, polls


def build(first, checks):
    rows = []
    for lid, it in first.items():
        cs = [c for c in checks.get(lid, []) if not c["challenged"] and isinstance(c["status"], int)]
        if not cs:
            continue
        lt = next((c["list_time"] / 1000 for c in cs if c.get("list_time")), None)
        start = min(lt, it["first_seen"]) if lt else it["first_seen"]
        event_t, last_alive = None, None
        for c in sorted(cs, key=lambda c: c["ts"]):
            gone = c["status"] == 410 or (c["status"] == 200 and c.get("ad_status") not in (None, "active"))
            if gone:
                event_t = ts(c["ts"])
                break
            if c["alive"]:
                last_alive = ts(c["ts"])
        if event_t is None and last_alive is None:
            continue
        end = event_t if event_t is not None else last_alive
        subj = (it.get("subject") or "").lower()
        rows.append(dict(list_id=lid, cat=it["cat"], T=max(end - start, 60) / 3600, E=int(event_t is not None),
                         price=it.get("price") or np.nan, dealer=int(bool(it.get("company_ad"))),
                         used=int(it.get("elt_condition") == 2), imgs=it.get("number_of_images") or 0,
                         log_sold_ads=np.log1p(it.get("sold_ads") or 0), verified=int(bool(it.get("is_shop_verified"))),
                         **{"f_" + k: int(bool(re.search(p, subj))) for k, p in FLAGS.items()}))
    df = pd.DataFrame(rows)
    df = df[df["price"] > 1000].reset_index(drop=True)
    # price relative to the median of the same category in this cohort (log)
    df["log_price_ratio"] = np.log(df["price"]) - df.groupby("cat")["price"].transform(lambda p: np.log(p).median())
    for c in df["cat"].unique():
        df["cat_" + c] = (df["cat"] == c).astype(int)
    return df


def main():
    raw, res = Path(sys.argv[1]), Path(sys.argv[2])
    first, checks, polls = load(raw)
    out = dict(stream=dict(listings=len(first)))
    for url, ps in polls.items():
        hrs = (ts(ps[-1]["ts"]) - ts(ps[0]["ts"])) / 3600 if len(ps) > 1 else 0
        out["stream"][url.rsplit("/", 1)[-1]] = dict(polls=len(ps), new_per_hour=round(sum(p["new"] for p in ps) / hrs, 1) if hrs else None,
                                                     saturated_share=round(np.mean([p["new"] >= p["n"] for p in ps]), 3))
    df = build(first, checks)
    out["cohort"] = dict(listings=len(df), events=int(df["E"].sum()), censored=int((1 - df["E"]).sum()),
                         followup_hours_median=round(float(df["T"].median()), 2), followup_hours_max=round(float(df["T"].max()), 2))
    km = KaplanMeierFitter().fit(df["T"], df["E"])
    tmax = float(df["T"].max())
    hz = [h for h in (1, 2, 3, 4, 6, 12, 24) if h <= tmax]  # no survival read-out past the longest follow-up
    out["kaplan_meier"] = {f"S({h}h)": round(float(km.predict(h)), 3) for h in hz}
    out["kaplan_meier"]["median_hours"] = None if np.isinf(km.median_survival_time_) else round(float(km.median_survival_time_), 2)
    out["kaplan_meier_by_cat"] = {c: {f"S({h}h)": round(float(KaplanMeierFitter().fit(g["T"], g["E"]).predict(h)), 3) for h in hz} | dict(n=len(g), events=int(g["E"].sum()))
                                  for c, g in df.groupby("cat")}
    feats = ["log_price_ratio", "dealer", "used", "imgs", "log_sold_ads", "verified"] + ["f_" + k for k in FLAGS] + \
            [c for c in df.columns if c.startswith("cat_")][1:]
    feats = [f for f in feats if df[f].std() > 0]
    kf = KFold(5, shuffle=True, random_state=0)
    ci = defaultdict(list)
    for tr, te in kf.split(df):
        a, b = df.iloc[tr], df.iloc[te]
        for name, cols in (("cox_all_features", feats), ("cox_price_ratio_only", ["log_price_ratio"])):
            m = CoxPHFitter(penalizer=0.1).fit(a[cols + ["T", "E"]], "T", "E")
            ci[name].append(concordance_index(b["T"], -m.predict_partial_hazard(b[cols]), b["E"]))
    out["cox_cv_concordance"] = {k: dict(mean=round(float(np.mean(v)), 3), sd=round(float(np.std(v)), 3)) for k, v in ci.items()}
    full = CoxPHFitter(penalizer=0.1).fit(df[feats + ["T", "E"]], "T", "E")
    out["cox_hazard_ratios"] = {k: round(float(v), 3) for k, v in full.hazard_ratios_.items()}
    out["cox_p_values"] = {k: round(float(v), 4) for k, v in full.summary["p"].items()}
    # classifier: removed within H hours, among listings observed for at least H hours or removed earlier
    if os.environ.get("STATS_ONLY") == "1":  # Cho Tot: ai-train=no, no learned classifier
        json.dump(out, open(res / "chotot_survival_bench.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(json.dumps(out, ensure_ascii=False, indent=1))
        return
    import lightgbm as lgb
    H = 3 if tmax < 8 else 6
    lab = df[(df["E"] == 1) & (df["T"] <= H) | (df["T"] >= H)].copy()
    lab["y"] = ((lab["E"] == 1) & (lab["T"] <= H)).astype(int)
    if lab["y"].nunique() == 2 and len(lab) >= 50:
        p = np.zeros(len(lab))
        for tr, te in kf.split(lab):
            clf = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.05, num_leaves=15, min_child_samples=10, verbose=-1)
            clf.fit(lab.iloc[tr][feats], lab.iloc[tr]["y"])
            p[te] = clf.predict_proba(lab.iloc[te][feats])[:, 1]
        out[f"lgbm_removed_within_{H}h"] = dict(n=len(lab), positives=int(lab["y"].sum()), cv_auc=round(float(roc_auc_score(lab["y"], p)), 3))
    else:
        out[f"lgbm_removed_within_{H}h"] = dict(n=len(lab), positives=int(lab["y"].sum()), cv_auc=None, note="not enough follow-up yet")
    json.dump(out, open(res / "chotot_survival_bench.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()

"""Retail domain benchmark (DMX every 10 min, Tiki every 2 h).

Speed, change detection on each offer's price series:
  S1 rule  price != last price (the current speed rule)
  S2 CUSUM on relative price change, S3 Page-Hinkley (river-style, own code)
  Ground truth = every observed price change. Recall, false alarms, delay in observations, per-event latency.
Batch, per-offer price anomaly on the same series with injected one-observation spikes (x0.6 / x1.5):
  B1 rolling MAD robust z (the current batch rule, window 12), B2 rolling-median % deviation (|p/med - 1| > 15%),
  B3 Hampel filter (window 7, 3 sigma). Share of series where MAD = 0 is reported (the rule cannot score them).
Batch, demand signal density on Tiki: share of products whose quantity_sold rose between consecutive passes.

usage: python retail_bench.py <raw_dir> <results_dir>
"""
import json, sys, time
from collections import defaultdict
from pathlib import Path

import numpy as np


def series(raw, job):
    """{offer id: [(pass number, price, item)]}; a pass ends when a URL comes round again."""
    s = defaultdict(list)
    seen, pass_no = set(), 0
    for line in open(raw / f"{job}.jsonl", encoding="utf-8"):
        r = json.loads(line)
        if r["url"] in seen:
            pass_no += 1
            seen = set()
        seen.add(r["url"])
        if r["status"] != 200:
            continue
        for it in r["items"]:
            if it.get("price"):
                s[str(it["id"])].append((pass_no, float(it["price"]), it))
    # one observation per pass: keep the first time an id is seen in a pass timestamp bucket
    out = {}
    for k, v in s.items():
        v.sort(key=lambda x: x[0])
        dedup, last_ts = [], None
        for ts, p, it in v:
            if ts != last_ts:
                dedup.append((ts, p, it))
            last_ts = ts
        out[k] = dedup
    return out


def cusum(x, k=0.01, h=0.05):
    """Two-sided CUSUM on relative changes; returns alarm indices."""
    alarms, gp, gn = [], 0.0, 0.0
    for i in range(1, len(x)):
        r = x[i] / x[i - 1] - 1
        gp, gn = max(0, gp + r - k), max(0, gn - r - k)
        if gp > h or gn > h:
            alarms.append(i)
            gp = gn = 0.0
    return alarms


def page_hinkley(x, delta=0.005, lam=0.05):
    alarms, mean, n, mt, mmin, mt2, mmax = [], 0.0, 0, 0.0, 0.0, 0.0, 0.0
    for i in range(1, len(x)):
        r = x[i] / x[0] - 1
        n += 1
        mean += (r - mean) / n
        mt += r - mean - delta
        mt2 += r - mean + delta
        mmin, mmax = min(mmin, mt), max(mmax, mt2)
        if mt - mmin > lam or mmax - mt2 > lam:
            alarms.append(i)
            mean, n, mt, mmin, mt2, mmax = 0.0, 0, 0.0, 0.0, 0.0, 0.0
            x = x[:]  # restart reference at the alarm
            x[0] = x[i]
    return alarms


def change_bench(S, min_len=6):
    res = {}
    events = {k: [i for i in range(1, len(v)) if v[i][1] != v[i - 1][1]] for k, v in S.items() if len(v) >= min_len}
    n_obs = sum(len(S[k]) - 1 for k in events)
    n_ev = sum(len(e) for e in events.values())
    for name in ("S1_rule_neq_last", "S2_cusum", "S3_page_hinkley"):
        tp = fa = 0
        delays, lat = [], []
        for k, ev in events.items():
            x = [p for _, p, _ in S[k]]
            t0 = time.perf_counter_ns()
            if name == "S1_rule_neq_last":
                al = [i for i in range(1, len(x)) if x[i] != x[i - 1]]
            elif name == "S2_cusum":
                al = cusum(x)
            else:
                al = page_hinkley(list(x))
            lat.append((time.perf_counter_ns() - t0) / max(1, len(x) - 1))
            used = set()
            for e in ev:
                hit = [a for a in al if e <= a <= e + 3 and a not in used]
                if hit:
                    tp += 1
                    used.add(hit[0])
                    delays.append(hit[0] - e)
            fa += len([a for a in al if a not in used])
        res[name] = dict(recall=round(tp / max(1, n_ev), 3), false_alarms=fa, false_alarm_per_1000_obs=round(1000 * fa / max(1, n_obs), 2),
                         mean_delay_obs=round(float(np.mean(delays)), 2) if delays else None,
                         latency_us_per_obs=round(float(np.median(lat)) / 1000, 2))
    return dict(series=len(events), observations=n_obs, change_events=n_ev,
                series_with_change=sum(1 for e in events.values() if e),
                rel_change_abs_median=round(float(np.median([abs(S[k][i][1] / S[k][i - 1][1] - 1) for k, e in events.items() for i in e])), 4) if n_ev else None,
                methods=res)


def rolling_flags(x, w, method):
    flags, scorable = [], 0
    for i in range(len(x)):
        hist = x[max(0, i - w):i]
        if len(hist) < 5:
            flags.append(False)
            continue
        med = float(np.median(hist))
        if method == "B1_rolling_mad_z":
            mad = float(np.median(np.abs(np.array(hist) - med)))
            if mad == 0:
                flags.append(False)  # INSUFFICIENT_DISPERSION in the current batch rule
                continue
            scorable += 1
            flags.append(abs(x[i] - med) / (1.4826 * mad) > 3.5)
        elif method == "B1b_current_rule_mad_then_iqr":
            # batch_layer/marketplace_anomaly.py: MAD first, IQR fence only when MAD is 0, no verdict when both are 0
            arr = np.array(hist)
            mad = float(np.median(np.abs(arr - med)))
            q1, q3 = np.percentile(arr, [25, 75])
            if mad > 0:
                scorable += 1
                flags.append(abs(x[i] - med) / (1.4826 * mad) > 3.5)
            elif q3 > q1:
                scorable += 1
                flags.append(x[i] < q1 - 1.5 * (q3 - q1) or x[i] > q3 + 1.5 * (q3 - q1))
            else:
                flags.append(False)
        elif method == "B2_rolling_median_pct":
            scorable += 1
            flags.append(abs(x[i] / med - 1) > 0.15)
        else:  # Hampel: MAD with a floor so that a flat history still scores
            mad = max(float(np.median(np.abs(np.array(hist) - med))), 0.01 * med)
            scorable += 1
            flags.append(abs(x[i] - med) / (1.4826 * mad) > 3)
    return flags, scorable


def spike_bench(S, seeds=10, min_len=12):
    keys = [k for k, v in S.items() if len(v) >= min_len]
    agg = defaultdict(lambda: defaultdict(list))
    for seed in range(seeds):
        rng = np.random.default_rng(seed)
        for name, w in (("B1_rolling_mad_z", 12), ("B1b_current_rule_mad_then_iqr", 12), ("B2_rolling_median_pct", 12), ("B3_hampel", 7)):
            tp = fp = fn = sc = tot = 0
            for k in keys:
                x = [p for _, p, _ in S[k]]
                y = [False] * len(x)
                i = int(rng.integers(6, len(x)))
                x[i] *= float(rng.choice([0.6, 1.5]))
                y[i] = True
                f, s = rolling_flags(x, w, name)
                sc += s
                tot += max(0, len(x) - 5)
                tp += sum(a and b for a, b in zip(f, y))
                fp += sum(a and not b for a, b in zip(f, y))
                fn += sum(b and not a for a, b in zip(f, y))
            p = tp / (tp + fp) if tp + fp else 0.0
            r = tp / (tp + fn) if tp + fn else 0.0
            for m, v in dict(precision=p, recall=r, f1=2 * p * r / (p + r) if p + r else 0.0, scorable_share=sc / max(1, tot)).items():
                agg[name][m].append(v)
    return dict(series=len(keys), methods={n: {m: dict(mean=round(float(np.mean(v)), 3), sd=round(float(np.std(v)), 3)) for m, v in ms.items()}
                                          for n, ms in agg.items()})


def demand_density(S):
    passes = sorted({ts for v in S.values() for ts, _, _ in v})
    rows = []
    for a, b in zip(passes, passes[1:]):
        up = tot = units = both = moved = 0
        for v in S.values():
            d = {ts: it for ts, _, it in v}
            if a in d and b in d:
                both += 1
                moved += d[a]["price"] != d[b]["price"]
            if a in d and b in d and d[a].get("sold") is not None and d[b].get("sold") is not None:
                tot += 1
                delta = d[b]["sold"] - d[a]["sold"]
                if delta > 0:
                    up += 1
                    units += delta
        rows.append(dict(from_ts=a, to_ts=b, products_with_sold=tot, sold_up=up, share_up=round(up / max(1, tot), 4), units=units,
                         products_in_both=both, price_changed=moved, price_changed_share=round(moved / max(1, both), 4)))
    return rows


def main():
    raw, res = Path(sys.argv[1]), Path(sys.argv[2])
    out = {}
    for job in ("dmx", "tiki"):
        S = series(raw, job)
        out[job] = dict(products=len(S), max_len=max((len(v) for v in S.values()), default=0),
                        change_detection=change_bench(S, min_len=6 if job == "dmx" else 3))
        if job == "dmx":
            out[job]["spike_detection"] = spike_bench(S)
        else:
            out[job]["demand_density"] = demand_density(S)
            flat = [it for v in S.values() for _, _, it in v[:1]]
            out[job]["has_sold_share"] = round(sum(1 for it in flat if it.get("sold") is not None) / max(1, len(flat)), 3)
    json.dump(out, open(res / "retail_bench.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(out, ensure_ascii=False, indent=1)[:6000])


if __name__ == "__main__":
    main()

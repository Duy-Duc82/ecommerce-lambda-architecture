"""Cross-site product matching benchmark (laptops): FPT Shop -> CellphoneS and FPT Shop -> Websosanh.

Silver labels: two URLs match when they share a manufacturer part-number token that is unique inside
each catalog (e.g. "eb027w" in "...x1404ma-eb027w"). Methods are scored twice: on the full name, and with
that per-configuration code masked (family codes such as x1404ma stay), as for sources without codes.

usage: python entity_bench.py <raw/entity_sitemaps.json> <results/entity_bench.json>
"""
import json, re, sys, time
from collections import Counter

import numpy as np
from rapidfuzz import fuzz, process
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GroupKFold

SPEC = [r"\d+(gb|tb|w|hz|mah|inch|cpu|gpu|core|k)", r"20\d\d", r"(i|r|u)[3579]", r"m\d(pro|max)?", r"\d{4,5}[a-z]{1,3}",
        r"[a-z]?\d{3}[a-z]{0,2}", r"(rtx|gtx|rx|mx|arc)\d+[a-z]*", r"gen\d+", r"wifi\d+[a-z]*", r"\d+(th|nd|rd)",
        r"ddr\d+[a-z]*", r"lpddr\d+[a-z]*", r"\d+[a-z]", r"[a-z]\d"]
SPEC_RE = re.compile(r"^(?:%s)$" % "|".join(SPEC))
USED = {"cu", "like", "new99", "tray", "xuoc", "dep", "openbox", "tbh", "trung", "bay"}


def tokens(url, site):
    s = url.lower().rstrip("/")
    if site == "websosanh":
        s = s.split("/")[-3]
    else:
        s = s.split("/")[-1].replace(".html", "")
    return s.split("-")


def is_pn(t):
    return len(t) >= 5 and re.fullmatch(r"[a-z0-9]+", t) and re.search(r"[a-z]", t) and len(re.findall(r"\d", t)) >= 2 \
        and not SPEC_RE.match(t)


def catalog(urls, site):
    rows = []
    for u in urls:
        tk = tokens(u, site)
        if site != "fptshop" and not ({"laptop", "macbook"} & set(tk) or (site == "websosanh" and "notebook" in tk)):
            continue
        if USED & set(tk):  # second-hand / display units are a different offer, not a candidate
            continue
        rows.append(dict(url=u, tk=tk))
    df = Counter(t for r in rows for t in set(r["tk"]) if is_pn(t))
    for r in rows:
        r["pn_unique"] = {t for t in r["tk"] if is_pn(t) and df[t] == 1}
        words = [t for t in r["tk"] if t not in ("laptop", "may", "tinh", "xach", "tay")]
        r["full"] = " ".join(words)
        # mask only the per-configuration code; family codes shared by several configs (x1404ma) stay
        r["masked"] = " ".join(t for t in words if t not in r["pn_unique"])
    return rows


def labels(left, right):
    idx = {}
    for j, r in enumerate(right):
        for t in r["pn_unique"]:
            idx[t] = j
    gt = {}
    for i, l in enumerate(left):
        hit = {idx[t] for t in l["pn_unique"] if t in idx}
        if len(hit) == 1:
            gt[i] = hit.pop()
        elif not hit:
            gt[i] = None
    # only left items with a unique part number are scored; "no match" is credible only for them
    return {i: v for i, v in gt.items() if left[i]["pn_unique"]}


def prf(pred, gt):
    tp = sum(1 for i, g in gt.items() if g is not None and pred.get(i) == g)
    fp = sum(1 for i, g in gt.items() if pred.get(i) is not None and pred.get(i) != g)
    fn = sum(1 for i, g in gt.items() if g is not None and pred.get(i) != g)
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    return dict(precision=round(p, 3), recall=round(r, 3), f1=round(2 * p * r / (p + r), 3) if p + r else 0.0, tp=tp, fp=fp, fn=fn)


def cv_threshold(scores, best, gt, folds):
    """Pick the threshold on training folds, apply on the held-out fold (no tuning on test items)."""
    items = sorted(gt)
    pred = {}
    for k in range(folds):
        test = set(items[k::folds])
        train = [i for i in items if i not in test]
        cands = sorted({scores[i] for i in train})
        th = max(cands, key=lambda t: prf({i: best[i] if scores[i] >= t else None for i in train}, {i: gt[i] for i in train})["f1"])
        for i in test:
            pred[i] = best[i] if scores[i] >= th else None
    return pred


def pair_features(l, r, cos_c, cos_w):
    lt, rt = set(l["masked"].split()), set(r["masked"].split())
    num_l = {t for t in lt if re.search(r"\d", t)}
    num_r = {t for t in rt if re.search(r"\d", t)}
    return [cos_c, cos_w, fuzz.token_set_ratio(l["masked"], r["masked"]) / 100, fuzz.partial_ratio(l["masked"], r["masked"]) / 100,
            len(lt & rt) / max(1, len(lt | rt)), len(num_l & num_r) / max(1, len(num_l | num_r)),
            float(l["tk"][0] == r["tk"][0] or l["tk"][0] in rt), abs(len(lt) - len(rt)) / max(len(lt), len(rt), 1)]


def run(left, right, text, folds=5, k=10):
    gt = labels(left, right)
    for row in left + right:
        row["masked"] = row[text]
    L = [l["masked"] for l in left]
    R = [r["masked"] for r in right]
    out = dict(left=len(left), right=len(right), scored_left=len(gt), with_match=sum(v is not None for v in gt.values()))
    items = sorted(gt)

    t0 = time.perf_counter()
    vc = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), sublinear_tf=True).fit(L + R)
    S_c = (vc.transform([L[i] for i in items]) @ vc.transform(R).T).toarray()
    t_c = time.perf_counter() - t0
    t0 = time.perf_counter()
    vw = TfidfVectorizer(token_pattern=r"[^ ]+", sublinear_tf=True).fit(L + R)
    S_w = (vw.transform([L[i] for i in items]) @ vw.transform(R).T).toarray()
    t_w = time.perf_counter() - t0

    res = {}
    for name, S, secs in (("tfidf_char_3_5", S_c, t_c), ("tfidf_word", S_w, t_w)):
        best = {i: int(S[n].argmax()) for n, i in enumerate(items)}
        sc = {i: float(S[n].max()) for n, i in enumerate(items)}
        res[name] = prf(cv_threshold(sc, best, gt, folds), gt) | dict(seconds=round(secs, 3))

    t0 = time.perf_counter()
    best, sc = {}, {}
    for i in items:
        m = process.extractOne(L[i], R, scorer=fuzz.token_set_ratio)
        best[i], sc[i] = m[2], m[1]
    res["rapidfuzz_token_set"] = prf(cv_threshold(sc, best, gt, folds), gt) | dict(seconds=round(time.perf_counter() - t0, 3))

    # blocking: top-k by char TF-IDF, then a learned pair scorer over the candidates
    topk = {i: list(np.argsort(-S_c[n])[:k]) for n, i in enumerate(items)}
    pos = {i: gt[i] for i in items}
    out["blocking_recall_at_%d" % k] = round(np.mean([pos[i] in topk[i] for i in items if pos[i] is not None]), 3)
    t0 = time.perf_counter()
    X, y, g, cand = [], [], [], []
    for n, i in enumerate(items):
        for j in topk[i]:
            X.append(pair_features(left[i], right[j], S_c[n, j], S_w[n, j]))
            y.append(int(pos[i] == j))
            g.append(i)
            cand.append((i, j))
    X, y, g = np.array(X), np.array(y), np.array(g)
    proba = np.zeros(len(y))
    for tr, te in GroupKFold(n_splits=folds).split(X, y, g):
        clf = LogisticRegression(max_iter=2000, class_weight="balanced").fit(X[tr], y[tr])
        proba[te] = clf.predict_proba(X[te])[:, 1]
    best, sc = {}, {}
    for (i, j), p in zip(cand, proba):
        if p > sc.get(i, -1):
            best[i], sc[i] = int(j), float(p)
    res["block_tfidf_top%d_logreg" % k] = prf(cv_threshold(sc, best, gt, folds), gt) | dict(seconds=round(time.perf_counter() - t0 + t_c, 3))

    # part numbers kept: exact unique-token rule over non-scored items too, as coverage
    out["part_number_coverage_left"] = round(sum(1 for l in left if l["pn_unique"]) / len(left), 3)
    out["methods"] = res
    out["examples"] = [dict(left=left[i]["url"].split("/")[-1], right=right[gt[i]]["url"].split("/")[-1 if "websosanh" not in right[gt[i]]["url"] else -3])
                       for i in items[:400:20] if gt[i] is not None]
    return out


if __name__ == "__main__":
    d = json.load(open(sys.argv[1], encoding="utf-8"))
    fpt = catalog(d["fptshop"], "fptshop")
    report = dict(note=__doc__.strip().split("\n\n")[1])
    for site in ("cellphones", "websosanh"):
        right = catalog(d[site], site)
        for text in ("full", "masked_config_code"):
            key = f"fptshop_to_{site}__{text}"
            for row in fpt + right:
                row.setdefault("masked_config_code", row["masked"])
            report[key] = run(fpt, right, text)
            print(key, json.dumps({k: v for k, v in report[key].items() if k != "examples"}, ensure_ascii=False), flush=True)
    report["silver_label_manual_check"] = "50/50 random positive pairs (25 per site) read by hand: all the same configuration"
    json.dump(report, open(sys.argv[2], "w", encoding="utf-8"), ensure_ascii=False, indent=1)

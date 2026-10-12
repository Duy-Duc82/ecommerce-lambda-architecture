"""Stepped rate-limit probe for one source: fixed request delay per step, ramped down
(e.g. 10 s -> 5 s -> 3 s -> 2 s -> 1 s), one request at a time, one host per process.
A step stops at the first sign of throttling (non-200, missing data marker, page far smaller
than normal). After a block: one recovery probe every 2 min, up to 30 min, then stop.
No concurrency, no proxies, no header rotation; robots.txt-allowed URLs only.

usage: python ratelimit.py <tiki_cat|tiki_prod|wss|chotot> <out_dir>
"""
import re, json, time, sys, random, gzip, ssl, urllib.request, urllib.error
from datetime import datetime, timezone

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
KEEP_HEADERS = ("retry-after", "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset", "server",
                "cf-cache-status", "x-cache", "cf-mitigated", "set-cookie")


def fetch(u):
    r = urllib.request.Request(u, headers={"User-Agent": UA, "Accept-Language": "vi-VN", "Accept-Encoding": "gzip"})
    t0 = time.time()
    try:
        x = urllib.request.urlopen(r, timeout=30, context=ssl.create_default_context())
        st = x.status
    except urllib.error.HTTPError as e:
        x, st = e, e.code
    except Exception as e:
        return dict(status="ERR:" + type(e).__name__, secs=round(time.time() - t0, 2), size=0, headers={}), ""
    b = x.read()
    if x.headers.get("Content-Encoding") == "gzip":
        b = gzip.decompress(b)
    hd = {k.lower(): v[:80] for k, v in x.headers.items() if k.lower() in KEEP_HEADERS}
    if "set-cookie" in hd:
        hd["set-cookie"] = hd["set-cookie"].split("=")[0]  # cookie name only
    h = b.decode("utf-8", "replace")
    return dict(status=st, secs=round(time.time() - t0, 2), size=len(h), headers=hd), h


def locs(sm):
    _, h = fetch(sm)
    return re.findall(r"<loc>\s*(.*?)\s*</loc>", h)


def next_data(h):
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', h, re.S)
    try:
        return json.loads(m.group(1)) if m else None
    except ValueError:
        return None


def longest_list(j, need):
    best, st = [], [j]
    while st:
        x = st.pop()
        if isinstance(x, list):
            if len(x) >= 3 and all(isinstance(i, dict) for i in x[:3]) and all(need in i for i in x[:3]):
                best = x if len(x) > len(best) else best
            st += x
        elif isinstance(x, dict):
            st += list(x.values())
    return best


# --- per-source: URL pool, page check -> (ok, n_items, note) ---------------------------------

def pool_tiki_cat():
    return locs("https://tiki.vn/clover/sitemap_categories_1.xml")


def check_tiki_cat(h, meta):
    j = next_data(h)
    if j is None:
        return False, 0, "no __NEXT_DATA__"
    return True, len(longest_list(j, "price")), ""


def pool_tiki_prod():
    return locs("https://tiki.vn/clover/sitemap_products_1.xml")


def check_tiki_prod(h, meta):
    if "__NEXT_DATA__" not in h:
        return False, 0, "challenge page" if meta["size"] < 40000 else "no __NEXT_DATA__"
    return True, int('"price"' in h), ""


def pool_wss():
    return locs("https://websosanh.vn/product-dien-thoai-may-tinh-bang-sitemap.xml") + \
        locs("https://websosanh.vn/product-tin-hoc-sitemap.xml")


def check_wss(h, meta):
    if "ld+json" not in h or "so-sanh" not in h:
        return False, 0, "no JSON-LD"
    m = re.search(r'"offerCount"\s*:\s*"?(\d+)', h)
    return True, int(m.group(1)) if m else 0, ""


CHOTOT_CATS = ["mua-ban-dien-thoai", "mua-ban-laptop", "mua-ban-do-dien-tu", "mua-ban-may-tinh-bang",
               "mua-ban-tivi-am-thanh", "mua-ban-tu-lanh", "mua-ban-may-giat", "mua-ban-xe-may",
               "mua-ban-may-anh-may-quay", "mua-ban-do-gia-dung-noi-that-cay-canh", "mua-ban", "mua-ban-xe-dap"]


def pool_chotot():
    # page 1 of each category only: robots.txt disallows /*page= for User-agent: *
    ids = []
    for c in CHOTOT_CATS:
        _, h = fetch(f"https://www.chotot.com/{c}")
        ids += [i["list_id"] for i in longest_list(next_data(h) or {}, "list_id")]
        time.sleep(6)
    urls = [f"https://www.chotot.com/{i}.htm" for i in dict.fromkeys(ids)]
    return urls + [f"https://www.chotot.com/{c}" for c in CHOTOT_CATS]


def check_chotot(h, meta):
    if meta["status"] == 410:
        return True, 0, "410 gone (listing removed)"
    j = next_data(h)
    if j is None:
        return False, 0, "no __NEXT_DATA__"
    n = len(longest_list(j, "list_id"))
    return True, n or int('"list_id"' in h), ""


SOURCES = {
    # name: (pool, check, delays per step, requests per step)
    "tiki_cat": (pool_tiki_cat, check_tiki_cat, [5, 3, 2, 1], 25),
    "tiki_prod": (pool_tiki_prod, check_tiki_prod, [10, 7, 5, 3], 25),  # earlier: blocked at 2 s, fine at 15 s
    "wss": (pool_wss, check_wss, [10, 5, 3, 2, 1], 25),
    "chotot": (pool_chotot, check_chotot, [10, 5, 3, 2, 1], 25),
}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main():
    name, out_dir = sys.argv[1], sys.argv[2]
    pool_fn, check, delays, per_step = SOURCES[name]
    pool = pool_fn()
    random.seed(11)
    random.shuffle(pool)
    print(name, "pool", len(pool), flush=True)
    log = open(f"{out_dir}/ratelimit_{name}.jsonl", "a", encoding="utf-8")
    it = iter(pool)
    steps, blocked = [], None
    normal_sizes = []
    for d in delays:
        rows, bad_run = [], 0
        for k in range(per_step):
            time.sleep(d)
            u = next(it)
            meta, h = fetch(u)
            ok, n, note = (False, 0, "http") if meta["status"] not in (200, 404, 410) else check(h, meta)
            if ok and meta["status"] == 200:
                normal_sizes.append(meta["size"])
            rec = dict(ts=now(), step_delay=d, i=k, url=u, ok=ok, n=n, note=note, **meta)
            log.write(json.dumps(rec, ensure_ascii=False) + "\n")
            log.flush()
            rows.append(rec)
            bad_run = 0 if ok else bad_run + 1
            if not ok:
                print(name, d, k, meta["status"], meta["size"], note, flush=True)
            if bad_run >= 2:
                blocked = dict(delay=d, at_request_in_step=k + 1,
                               total_requests_before=sum(len(s["rows"]) for s in steps) + k + 1,
                               status=meta["status"], size=meta["size"], note=note, headers=meta["headers"], ts=rec["ts"])
                break
        steps.append(dict(delay=d, rows=rows))
        okn = sum(r["ok"] for r in rows)
        print(name, f"step {d}s: {okn}/{len(rows)} ok", flush=True)
        if blocked:
            break
    recovery = None
    if blocked:
        t_block = time.time()
        while time.time() - t_block < 30 * 60:
            time.sleep(120)
            meta, h = fetch(next(it))
            ok, n, note = (False, 0, "http") if meta["status"] not in (200, 404, 410) else check(h, meta)
            log.write(json.dumps(dict(ts=now(), phase="recovery", ok=ok, note=note, **meta), ensure_ascii=False) + "\n")
            if ok:
                recovery = dict(minutes_after_block=round((time.time() - t_block) / 60, 1), ts=now())
                break
        print(name, "recovery", recovery, flush=True)

    def summ(s):
        r = s["rows"]
        secs = sorted(x["secs"] for x in r)
        return dict(delay_s=s["delay"], requests=len(r), ok=sum(x["ok"] for x in r),
                    codes={str(c): sum(1 for x in r if x["status"] == c) for c in {x["status"] for x in r}},
                    median_secs=secs[len(secs) // 2], max_secs=secs[-1],
                    median_size=sorted(x["size"] for x in r)[len(r) // 2],
                    rate_headers=sorted({k for x in r for k in x["headers"] if k.startswith(("retry", "x-rate"))}))
    out = dict(source=name, started=steps[0]["rows"][0]["ts"], pool=len(pool), steps=[summ(s) for s in steps],
               blocked=blocked, recovery=recovery,
               fastest_clean_delay=min((s["delay"] for s in steps if all(x["ok"] for x in s["rows"])), default=None))
    json.dump(out, open(f"{out_dir}/ratelimit_{name}.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(out, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

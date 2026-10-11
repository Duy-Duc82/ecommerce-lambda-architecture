"""Extra sources (found by the second probe) in the same JSONL format as crawl_round.py,
plus a Tiki product-page canary to see when the JS challenge lifts."""
import sys, json, time, ssl, urllib.request, gzip
from datetime import datetime, timezone
sys.path.insert(0, ".")
import parsers_probe2 as P
from fetch import UA

OUT = sys.argv[1] if len(sys.argv) > 1 else "rounds.jsonl"
NOVERIFY = ssl.create_default_context()


def get(u):
    r = urllib.request.Request(u, headers={"User-Agent": UA, "Accept-Language": "vi-VN", "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(r, timeout=30) as x:
        b = x.read()
        if x.headers.get("Content-Encoding") == "gzip":
            b = gzip.decompress(b)
        return x.status, b.decode("utf-8", "replace")


def wrap(fn):
    def f(h):
        out = []
        for x in fn(h):
            x = dict(x)
            out.append(dict(id=x.pop("id"), name=x.pop("name"), price=x.pop("price"), extra=x))
        return [o for o in out if o["price"]]
    return f


def tiki_canary(h):
    import re
    ok = '__NEXT_DATA__' in h and len(h) > 50000
    return [dict(id="canary", name="tiki product page", price=1 if ok else 0, extra=dict(kb=len(h) // 1024))] if ok else []


SOURCES = [
    ("meta", "https://meta.vn/tivi-c3256", wrap(P.p_itemlist)),
    ("mediamart", "https://mediamart.vn/tivi", wrap(P.p_itemlist)),
    ("pico", "https://pico.vn/tivi.c", wrap(P.p_pico)),
    ("kingfoodmart", "https://kingfoodmart.com/trai-cay", wrap(P.p_kfm)),
    ("hoanghamobile", "https://hoanghamobile.com/dien-thoai-di-dong", wrap(P.p_hhm)),
    ("bonbanh", "https://bonbanh.com/oto", wrap(P.p_bonbanh)),
    ("tiki_product", "https://tiki.vn/sach-khu-vuon-bi-mat-p277362886.html", tiki_canary),
]

if __name__ == "__main__":
    only = set(sys.argv[2:])
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(OUT, "a", encoding="utf-8") as f:
        for site, url, parser in SOURCES:
            if only and site not in only:
                continue
            rec = dict(ts=ts, site=site, url=url)
            t0 = time.time()
            try:
                st, h = get(url)
                items = parser(h)
                rec.update(status=st, n=len(items), items=items)
            except Exception as e:
                rec.update(status="ERR", n=0, err=f"{type(e).__name__}: {str(e)[:150]}", items=[])
            rec["secs"] = round(time.time() - t0, 2)
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            print(f"{site:13} {rec['status']} n={rec['n']:3} {rec['secs']}s {rec.get('err', '')}", flush=True)
            time.sleep(5)

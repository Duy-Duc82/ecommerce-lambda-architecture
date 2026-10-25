"""Data collection for the ML/statistics benchmark (2026-10-17).

Plain HTTP GET, one request at a time per host, robots.txt-allowed URLs only, no internal APIs,
no challenge solving. Personal fields of Cho Tot listings (phone, names, avatar, address,
coordinates, body text) are never written.

usage: python collect.py <job> <raw_dir> [hours]
jobs:
  chotot_stream  page 1 of 3 Cho Tot categories every 3 min      -> chotot_stream.jsonl
  chotot_models  page 1 of 260 national phone-model pages every 1 h      -> chotot_models.jsonl
  chotot_track   detail page of every streamed listing, each 90 min -> chotot_track.jsonl
  dmx            4 DMX category pages every 10 min                -> dmx.jsonl
  tiki           300 fixed Tiki category pages every 2 h          -> tiki.jsonl
"""
import gzip, json, random, re, ssl, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
CTX = ssl.create_default_context()


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fetch(url):
    """Return (status, html, seconds). HTTP errors are returned, not raised."""
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "vi-VN", "Accept-Encoding": "gzip"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=30, context=CTX) as r:
            body, status, enc = r.read(), r.status, r.headers.get("Content-Encoding")
    except urllib.error.HTTPError as e:
        body, status, enc = e.read(), e.code, e.headers.get("Content-Encoding")
    except Exception as e:  # network error
        return f"ERR {type(e).__name__}", "", round(time.time() - t0, 2)
    if enc == "gzip":
        body = gzip.decompress(body)
    return status, body.decode("utf-8", "replace"), round(time.time() - t0, 2)


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


# ---- Cho Tot -------------------------------------------------------------------------------
CT_KEEP = ("list_id", "ad_id", "subject", "price", "date", "list_time", "category", "type", "mobile_brand",
           "mobile_model", "mobile_capacity", "mobile_color", "elt_condition", "company_ad", "region_name",
           "area_name", "number_of_images", "is_shop_verified", "official_store", "protection_entitlement",
           "sold_ads", "state", "status", "is_sticky", "sticky_ad_type", "contain_videos")


def ct_items(h):
    out = []
    for i in longest_list(next_data(h) or {}, "list_id"):
        d = {k: i.get(k) for k in CT_KEEP}
        d["params"] = {p.get("id"): p.get("value") for p in (i.get("params") or []) if isinstance(p, dict)}
        out.append(d)
    return out


CT_CATS = ["mua-ban-dien-thoai", "mua-ban-laptop", "mua-ban-do-dien-tu"]


def chotot_model_urls():
    urls = []
    for n in range(1, 7):
        _, x, _ = fetch(f"https://www.chotot.com/sitemap-dien-thoai-model-{n}.xml")
        urls += re.findall(r"<loc>\s*(https://www\.chotot\.com/mua-ban-dien-thoai-[^<\s]*-sdmd\d+ml\d+)\s*</loc>", x)
        time.sleep(3)
    # one page per model: the shortest URL is the national page, longer ones embed a province name
    by_model = {}
    for u in dict.fromkeys(urls):
        mid = re.search(r"sdmd(\d+)ml(\d+)$", u).groups()
        if mid not in by_model or len(u) < len(by_model[mid]):
            by_model[mid] = u
    apple = sorted(u for (b, _), u in by_model.items() if b == "1")
    other = sorted(u for (b, _), u in by_model.items() if b != "1" and re.search(r"-(samsung|oppo|xiaomi)-", u))
    random.Random(17).shuffle(other)
    return apple + sorted(other[:max(0, 260 - len(apple))])


def model_id(url):
    return int(re.search(r"ml(\d+)$", url).group(1))


# ---- DMX / Tiki ----------------------------------------------------------------------------
def dmx_items(h):
    import html as H
    out = []
    for a in re.findall(r"<a [^>]*data-id=[^>]*>", h):
        d = dict(re.findall(r'data-([\w-]+)="([^"]*)"', a))
        if d.get("price"):
            href = re.search(r'href="([^"]+)"', a)
            out.append(dict(id=d["id"], name=H.unescape(d.get("name", "")), price=float(d["price"]),
                            brand=d.get("brand"), cate=H.unescape(d.get("cate", "")),
                            flash=bool(href and "flashsale" in href.group(1))))
    return out or dmx_items_ld(h)


def dmx_items_ld(h):
    """Since 2026-10-22 DMX listings carry no data-* attributes; prices are in a JSON-LD ItemList.
    The product id is taken from the image path (/Images/<cat>/<id>/), the same id data-id carried."""
    out = []
    for blk in re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', h, re.S):
        try:
            j = json.loads(blk)
        except ValueError:
            continue
        els, st = [], [j]
        while st:  # the ItemList sits inside CollectionPage.mainEntity
            x = st.pop()
            if isinstance(x, dict):
                if x.get("@type") == "ItemList":
                    els += x.get("itemListElement") or []
                st += list(x.values())
            elif isinstance(x, list):
                st += x
        for el in els:
            it = (el.get("item") or {}) if isinstance(el, dict) else {}
            o = it.get("offers") or {}
            img = json.dumps(it.get("image") or "")
            m = re.search(r"/Images/\d+/(\d+)/", img)
            if it.get("@type") == "Product" and o.get("price"):
                out.append(dict(id=m.group(1) if m else it.get("url", "").rstrip("/").split("/")[-1], name=it.get("name"),
                                price=float(o["price"]), brand=None, cate=None, flash="flashsale" in it.get("url", ""),
                                avail=str(o.get("availability", "")).split("/")[-1]))
    return out


DMX_CATS = ["tivi", "tu-lanh", "may-giat", "may-lanh"]


def tiki_items(h):
    out = []
    for i in longest_list(next_data(h) or {}, "price"):
        if "id" not in i or i.get("advertisement"):
            continue
        out.append(dict(id=i["id"], name=i.get("name"), price=i.get("price"), orig=i.get("original_price"),
                        disc=i.get("discount_rate"), sold=(i.get("quantity_sold") or {}).get("value"),
                        rating=i.get("rating_average"), reviews=i.get("review_count"), seller=i.get("seller_name"),
                        brand=i.get("brand_name"), cat_path=i.get("primary_category_path")))
    return out


def tiki_cats():
    _, x, _ = fetch("https://tiki.vn/clover/sitemap_categories_1.xml")
    cats = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
    return random.Random(42).sample(cats, 300)


# ---- jobs ----------------------------------------------------------------------------------
def write(f, rec):
    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    f.flush()


def run_pass(f, urls, parse, delay, job):
    for u in urls:
        st, h, secs = fetch(u)
        items = parse(h) if st == 200 else []
        if job == "chotot_models":  # a model with no listings is padded with unrelated ones: drop those
            items = [i for i in items if i.get("mobile_model") == model_id(u)]
        write(f, dict(ts=now(), job=job, url=u, status=st, secs=secs, size=len(h), n=len(items), items=items))
        time.sleep(delay)


def main():
    job, raw = sys.argv[1], Path(sys.argv[2])
    hours = float(sys.argv[3]) if len(sys.argv) > 3 else 8
    stop = time.time() + hours * 3600
    raw.mkdir(parents=True, exist_ok=True)
    with open(raw / f"{job}.jsonl", "a", encoding="utf-8") as f:
        if job == "chotot_stream":
            while time.time() < stop:
                t0 = time.time()
                run_pass(f, [f"https://www.chotot.com/{c}" for c in CT_CATS], ct_items, 5, job)
                time.sleep(max(0, 180 - (time.time() - t0)))
        elif job == "chotot_models":
            urls = chotot_model_urls()
            while time.time() < stop:
                t0 = time.time()
                run_pass(f, urls, ct_items, 4, job)
                time.sleep(max(0, 3600 - (time.time() - t0)))
        elif job == "chotot_track":
            stream = raw / "chotot_stream.jsonl"
            time.sleep(int(__import__("os").environ.get("TRACK_FIRST_WAIT", 1800)))  # let the stream build a cohort first
            while time.time() < stop:
                t0 = time.time()
                ids = []
                for line in open(stream, encoding="utf-8"):
                    ids += [i["list_id"] for i in json.loads(line)["items"]]
                for lid in list(dict.fromkeys(ids)):
                    st, h, secs = fetch(f"https://www.chotot.com/{lid}.htm")
                    alive = st == 200 and str(lid) in h and next_data(h) is not None
                    j = next_data(h) if st == 200 else None
                    ad = None
                    if j:
                        ad = ((j.get("props") or {}).get("initialState") or {}).get("adView", {}).get("adInfo", {}).get("ad")
                    write(f, dict(ts=now(), job=job, list_id=lid, status=st, secs=secs, size=len(h), alive=alive,
                                  price=(ad or {}).get("price"), ad_status=(ad or {}).get("status"), list_time=(ad or {}).get("list_time"),
                                  challenged=st == 403))
                    time.sleep(2)
                time.sleep(max(0, int(__import__("os").environ.get("TRACK_EVERY", 5400)) - (time.time() - t0)))
        elif job == "dmx":
            while time.time() < stop:
                t0 = time.time()
                run_pass(f, [f"https://www.dienmayxanh.com/{c}" for c in DMX_CATS], dmx_items, 6, job)
                time.sleep(max(0, 600 - (time.time() - t0)))
        elif job == "tiki":
            urls = tiki_cats()
            while time.time() < stop:
                t0 = time.time()
                run_pass(f, urls, tiki_items, 4, job)
                time.sleep(max(0, int(__import__("os").environ.get("TIKI_EVERY", 7200)) - (time.time() - t0)))


if __name__ == "__main__":
    main()

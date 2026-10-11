"""One crawl round over the shortlisted sources.

Each source: 1-2 listing pages over plain HTTP (BHX: Playwright), parsed into
(id, name, price, extra). Appends one JSON line per source to rounds.jsonl:
{ts, site, url, status, secs, n, err, items:[...]}. No internal APIs, robots-allowed URLs only.
"""
import re, json, time, sys, html, traceback
from datetime import datetime, timezone
from fetch import get
from rsc import rsc

OUT = sys.argv[1] if len(sys.argv) > 1 else "rounds.jsonl"


def next_data(h):
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', h, re.S)
    return json.loads(m.group(1)) if m else None


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


def p_tiki(h):
    return [dict(id=i["id"], name=i["name"], price=i["price"],
                 extra=dict(orig=i.get("original_price"), sold=(i.get("quantity_sold") or {}).get("value"),
                            rating=i.get("rating_average"), reviews=i.get("review_count"), seller=i.get("seller_name")))
            for i in longest_list(next_data(h), "price") if "id" in i]


def p_chotot(h):
    # PII (phone, account_name, full_name) deliberately not kept
    return [dict(id=i["list_id"], name=i.get("subject"), price=i.get("price"),
                 extra=dict(region=i.get("region_name"), cond=i.get("elt_condition"), model=i.get("mobile_model"),
                            age=i.get("date")))
            for i in longest_list(next_data(h), "list_id")]


def p_phongvu(h):
    return [dict(id=i["sku"], name=i["name"], price=(i.get("price") or {}).get("latestPrice"),
                 extra=dict(retail=(i.get("price") or {}).get("supplierRetailPrice"), stock=i.get("stockQuantity")))
            for i in longest_list(next_data(h), "sku") if isinstance(i.get("price"), dict)]


def p_ld_itemlist(h):
    out = []
    for blk in re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', h, re.S):
        try:
            j = json.loads(blk)
        except Exception:
            continue
        st = [j]
        while st:
            x = st.pop()
            if isinstance(x, list):
                st += x
            elif isinstance(x, dict):
                if x.get("@type") == "Product" and x.get("url"):
                    o = x.get("offers") or {}
                    o = o[0] if isinstance(o, list) and o else o
                    ar = x.get("aggregateRating") or {}
                    out.append(dict(id=x["url"].rstrip("/").split("/")[-1], name=x.get("name"),
                                    price=o.get("price") or o.get("lowPrice"),
                                    extra=dict(rating=ar.get("ratingValue"), reviews=ar.get("ratingCount"),
                                               avail=str(o.get("availability", "")).split("/")[-1])))
                st += list(x.values())
    return out


def p_dmx(h):
    out = []
    for a in re.findall(r"<a [^>]*data-id=[^>]*>", h):
        d = dict(re.findall(r'data-([\w-]+)="([^"]*)"', a))
        if d.get("price"):
            out.append(dict(id=d["id"], name=html.unescape(d.get("name", "")), price=float(d["price"]),
                            extra=dict(brand=d.get("brand"), cate=html.unescape(d.get("cate", "")))))
    return out


def p_fpt(h):
    s, dec, out = rsc(h), json.JSONDecoder(), []
    for m in re.finditer(r'\{"propertyNameOfUpc"', s):
        try:
            o, _ = dec.raw_decode(s, m.start())
        except ValueError:
            continue
        if o.get("code") and o.get("currentPrice"):
            out.append(dict(id=o["code"], name=o.get("name"), price=o["currentPrice"],
                            extra=dict(orig=o.get("originalPrice"), sold=(o.get("sold") or {}).get("realQuantity"),
                                       rating=o.get("averageScore"))))
    return out


def p_wss(h):
    out = []
    for blk in re.findall(r'<div class="product-single(?: border-top)?">(.*?)nơi bán', h, re.S):
        m = re.search(r'href="/[^"]+/(\d+)/so-sanh\.htm">([^<]+)</a>', blk)
        pr = re.search(r'Giá từ ([\d.]+) đ', blk)
        mc = re.search(r'merchant-number">&nbsp;(\d+)', blk)
        if m and pr:
            out.append(dict(id=m.group(1), name=html.unescape(m.group(2)), price=int(pr.group(1).replace(".", "")),
                            extra=dict(merchants=int(mc.group(1)) if mc else None)))
    return out


def p_bhx(url):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        pg = b.new_page(locale="vi-VN")
        pg.goto(url, wait_until="domcontentloaded", timeout=60000)
        pg.wait_for_timeout(8000)
        items = pg.eval_on_selector_all(
            "div.box_product",
            "els => els.map(e => ({href: (e.querySelector('a[href]') || {getAttribute: () => ''}).getAttribute('href'),"
            " t: e.innerText}))")
        b.close()
    out, seen = [], set()
    for o in items:
        pr = re.findall(r"(\d{1,3}(?:\.\d{3})+)\s*đ", o["t"])
        key = (o["href"] or "").split("?")[0]
        if not key or key in seen or not pr:
            continue
        seen.add(key)
        lines = [l for l in o["t"].split("\n") if len(l) > 12 and "đ" not in l]
        # first price on the card = current price, a second one (line-through) = list price
        out.append(dict(id=key.rstrip("/").split("/")[-1], name=lines[0] if lines else "",
                        price=int(pr[0].replace(".", "")),
                        extra=dict(orig=int(pr[1].replace(".", "")) if len(pr) > 1 else None)))
    return 200, out


SOURCES = [
    ("tiki", "https://tiki.vn/dien-thoai-may-tinh-bang/c1789", p_tiki),
    ("tiki", "https://tiki.vn/nha-sach-tiki/c8322", p_tiki),
    ("chotot", "https://www.chotot.com/mua-ban-dien-thoai", p_chotot),
    ("tgdd", "https://www.thegioididong.com/dtdd", p_ld_itemlist),
    ("dmx", "https://www.dienmayxanh.com/tivi", p_dmx),
    ("phongvu", "https://phongvu.vn/c/laptop", p_phongvu),
    ("fptshop", "https://fptshop.com.vn/dien-thoai", p_fpt),
    ("nguyenkim", "https://www.nguyenkim.com/dien-thoai-di-dong/", p_ld_itemlist),
    ("websosanh", "https://websosanh.vn/dien-thoai-samsung-galaxy/cat-88.htm", p_wss),
    ("bachhoaxanh", "https://www.bachhoaxanh.com/sua-tuoi", None),
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
                if parser is None:
                    st, items = p_bhx(url)
                else:
                    st, h = get(url)
                    items = parser(h)
                rec.update(status=st, n=len(items), items=items)
            except Exception as e:
                rec.update(status="ERR", n=0, err=f"{type(e).__name__}: {str(e)[:150]}", items=[])
            rec["secs"] = round(time.time() - t0, 2)
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            print(f"{site:12} {rec['status']} n={rec['n']:3} {rec['secs']}s {rec.get('err', '')}", flush=True)
            time.sleep(6)  # >= TGDD crawl-delay 5s between hosts too

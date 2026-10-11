"""Panel demo: for models listed on TGDD, Websosanh search -> first compare page -> AggregateOffer,
next to the TGDD listing price (JSON-LD) when present."""
import re, json, time, html
from fetch import get
from prodpage import ld_products

PANEL = [("samsung galaxy a27 6gb 128gb", "samsung-galaxy-a27-6gb-128gb"), ("samsung galaxy a37 8gb 128gb", "samsung-galaxy-a37-8gb-128gb"),
         ("samsung galaxy a57 8gb 256gb", "samsung-galaxy-a57-8gb-256gb"), ("samsung galaxy s26 ultra 12gb 256gb", "samsung-galaxy-s26-ultra-12gb-256gb"),
         ("iphone 17 256gb", "iphone-17"), ("iphone 17 pro 256gb", "iphone-17-pro"), ("iphone 17 pro max 256gb", "iphone-17-pro-max"),
         ("iphone 18 pro 256gb", "iphone-18-pro"), ("iphone 18 pro max 256gb", "iphone-18-pro-max"), ("oppo reno16 f", "oppo-reno16-f"),
         ("xiaomi 17 12gb 512gb", "xiaomi-17")]
rows = []
for kw, tg in PANEL:
    r = dict(model=kw)
    st, h = get("https://websosanh.vn/s/" + kw.replace(" ", "+"))
    m = re.search(r'href="(/[^"]+/(\d+)/so-sanh\.htm)">([^<]+)</a>', h)
    if m:
        time.sleep(3)
        st, c = get("https://websosanh.vn" + m.group(1))
        lp = ld_products(c)
        o = (lp[0].get("offers") or {}) if lp else {}
        r.update(wss_name=html.unescape(m.group(3))[:60], wss_low=o.get("lowPrice"), wss_high=o.get("highPrice"), wss_offers=o.get("offerCount"))
    time.sleep(6)
    st, t = get("https://www.thegioididong.com/dtdd/" + tg)
    lp = ld_products(t)
    if lp:
        o = lp[0].get("offers") or {}
        o = o[0] if isinstance(o, list) and o else o
        r.update(tgdd_name=(lp[0].get("name") or "")[:50], tgdd_price=o.get("price") or o.get("lowPrice"))
    rows.append(r)
    print(json.dumps(r, ensure_ascii=False), flush=True)
    time.sleep(6)
json.dump(rows, open("panel.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)

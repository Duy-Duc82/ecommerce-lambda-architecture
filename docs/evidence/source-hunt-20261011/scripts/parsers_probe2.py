import sys, re, json, html as H

def num(s):
    if s is None: return None
    d = re.sub(r"[^\d]", "", str(s).split(".00")[0] if str(s).endswith(".00") else str(s))
    return int(d) if d else None

def lds(t):
    out = []
    for ld in re.findall(r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', t, re.S):
        try: out.append(json.loads(ld.strip()))
        except Exception: pass
    return out

def nextdata(t):
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', t, re.S)
    return json.loads(m.group(1)) if m else None

def p_itemlist(t):
    r = []
    for j in lds(t):
        if isinstance(j, dict) and j.get("@type") == "ItemList":
            for e in j["itemListElement"]:
                it = e.get("item", e)
                of = it.get("offers") or {}
                ar = it.get("aggregateRating") or {}
                r.append(dict(id=it.get("sku") or it.get("mpn") or it.get("url"), name=it.get("name"), price=num(of.get("price")),
                              avail=(of.get("availability") or "").split("/")[-1], rating=ar.get("ratingValue"), reviews=ar.get("reviewCount"),
                              url=of.get("url") or it.get("url")))
    return r

def p_hhm(t):
    r = []
    for m in re.finditer(r'<h3>\s*<a title="([^"]+)" href="(/[^"]+)"', t):
        seg = t[m.end():m.end() + 2500]
        pm = re.search(r'<div class="price">\s*<strong>([\d,]+)\s*₫', seg)
        om = re.search(r'<strike>([\d,]+)\s*₫', seg)
        r.append(dict(id=m.group(2), name=H.unescape(m.group(1)), price=num(pm.group(1)) if pm else None, orig=num(om.group(1)) if om else None))
    return [x for x in r if x["price"]]

def p_pico(t):
    d = nextdata(t)["props"]["pageProps"]["pageDetail"]
    r = [dict(id=x.get("code") or x.get("_id"), name=x.get("name"), price=x.get("finalPrice") or x.get("salePrice"), orig=x.get("price"),
              stock=x.get("stockStatus"), orderQty=x.get("orderQuantity")) for x in d["data"]]
    return r

def p_dmtnh(t):
    r = []
    for m in re.finditer(r'<div class="card-cate" data-pid="(\d+)"', t):
        seg = t[m.end():m.end() + 3000]
        nm = re.search(r'<h3[^>]*>([^<]+)</h3>', seg); pm = re.search(r'data-price="([\d.]+)"', seg)
        om = re.search(r'secondary-price"><p>[^<]*</p><span>([\d,]+)', seg)
        r.append(dict(id=m.group(1), name=nm.group(1).strip() if nm else None, price=int(float(pm.group(1))) if pm else None, orig=num(om.group(1)) if om else None))
    return [x for x in r if x["price"]]

def p_coop(t):
    d = nextdata(t)["props"]["pageProps"]["serverProducts"]
    return [dict(id=x.get("sku"), name=x.get("name"), price=x.get("price"), stock=x.get("stockQuantity"), rating=x.get("averageRating"), uom=x.get("uom")) for x in d]

def p_kfm(t):
    d = nextdata(t)["props"]["pageProps"]["categoryData"]["products"]
    return [dict(id=x.get("id"), name=x.get("name"), price=x.get("discountPrice") or x.get("originalPrice"), orig=x.get("originalPrice"),
                 inStock=x.get("inStock"), ordered=x.get("orderedCounter")) for x in d]

def p_tz(t):
    r = {}
    for m in re.finditer(r'<a href=\'([^\']+)\'[^>]*data-name="([^"]+)" data-id="(\d+)" data-price="([\d.]+)"', t):
        r[m.group(3)] = dict(id=m.group(3), name=H.unescape(m.group(2)), price=int(float(m.group(4))), url=m.group(1))
    return list(r.values())

def p_mk(t):
    r = []
    for m in re.finditer(r'<a href="(/collections/[^"#]+/products/[^"#]+)#?" class="boost-pfs-filter-product-item-title">([^<]+)</a>', t):
        seg = t[m.end():m.end() + 600]
        pm = re.search(r'([\d.]+)₫', seg); om = re.search(r'<s>([\d.]+)₫', seg)
        r.append(dict(id=m.group(1).split("/products/")[-1], name=H.unescape(m.group(2)), price=num(pm.group(1)) if pm else None, orig=num(om.group(1)) if om else None))
    return r

P = dict(hhm=p_hhm, meta=p_itemlist, mm=p_itemlist, pico=p_pico, dmtnh=p_dmtnh, coop=p_coop, kfm=p_kfm, tz=p_tz, mk=p_mk)

def _main():
    site = sys.argv[1]
    sets = []
    for fn in sys.argv[2:]:
        r = P[site](open(fn, encoding="utf-8").read())
        wp = [x for x in r if x.get("price")]
        print(f"{site} {fn}: records={len(r)} with_price={len(wp)}")
        for x in wp[:2]: print("   ", json.dumps(x, ensure_ascii=False)[:300])
        sets.append(set(str(x["id"]) for x in r))
    if len(sets) == 2:
        print(f"   p1&p2 overlap={len(sets[0] & sets[1])} p2_new={len(sets[1] - sets[0])}")

def p_ak(t):
    r = {}
    for m in re.finditer(r'<a title="([^"]+)" class="line-clamp-3[^"]*" href="([^"]+)"', t):
        seg = t[m.end():m.end() + 3000]
        pm = re.search(r'<span>([\d.]+)₫</span>', seg); om = re.search(r'<del[^>]*>([\d.]+)₫', seg)
        sku = re.search(r'sku=(\d+)', m.group(2))
        r[m.group(2)] = dict(id=sku.group(1) if sku else m.group(2), name=H.unescape(m.group(1)), price=num(pm.group(1)) if pm else None, orig=num(om.group(1)) if om else None)
    return list(r.values())

def p_bibo(t):
    r = []
    for m in re.finditer(r'<h4 class="name-item"><a href="([^"]+)" title="([^"]+)"', t):
        seg = t[m.end():m.end() + 800]
        pm = re.search(r'price-item">([\d.]+)đ', seg); om = re.search(r'price-sale">([\d.]+)đ', seg); sm = re.search(r'offline-value">([\d.]+)đ', seg)
        r.append(dict(id=m.group(1), name=H.unescape(m.group(2)), price=num(pm.group(1)) if pm else None, orig=num(om.group(1)) if om else None, store_price=num(sm.group(1)) if sm else None))
    return r

def p_hc(t):
    r = []
    for m in re.finditer(r'<a class="a-CardView-fullLink" href="(/ords/product/[^"]+)"', t):
        seg = t[m.end():m.end() + 3000]
        nm = re.search(r'<h3>([^<]+)</h3>', seg); pm = re.search(r'hc_sale_price">\s*([\d,]+)đ', seg); om = re.search(r'hc_supp_price">\s*([\d,]+)đ', seg)
        r.append(dict(id=m.group(1).split("/")[-1], name=H.unescape(nm.group(1)) if nm else None, price=num(pm.group(1)) if pm else None, orig=num(om.group(1)) if om else None))
    return r
P.update(ak=p_ak, bibo=p_bibo, hc=p_hc)

def p_bonbanh(t):
    r = []
    for m in re.finditer(r'<li class="car-item[^"]*" itemscope itemtype="http://schema.org/Car">\s*<a itemprop="url" href="([^"]+)"', t):
        seg = t[m.end():m.end() + 2500]
        nm = re.search(r'<h3 itemprop="name">([^<]+)</h3>', seg); pm = re.search(r'itemprop="price" content="(\d+)"', seg)
        loc = re.search(r'<div class="cb4">\s*<b>([^<]+)</b>', seg)
        r.append(dict(id=m.group(1).rsplit("-", 1)[-1], name=H.unescape(nm.group(1)) if nm else None, price=int(pm.group(1)) if pm else None, loc=loc.group(1) if loc else None))
    return r

def p_oto(t):
    r = []
    for m in re.finditer(r'<div class="item-car[^"]*" data-tinrao="[^"]*" data-autoid="(\d+)"', t):
        seg = t[m.end():m.end() + 4000]
        nm = re.search(r'<span class="car-name">([^<]+)</span>', seg); pm = re.search(r'<p class="price[^"]*">\s*([^<]+?)\s*</p>', seg)
        km = re.search(r'single-line-and-hidden">\s*([^<]+?)\s*</li>', seg)
        r.append(dict(id=m.group(1), name=H.unescape(nm.group(1)) if nm else None, price=pm.group(1) if pm else None, km=km.group(1) if km else None))
    return r

def p_lm(t):
    r = {}
    for m in re.finditer(r'<div class="field-name[^"]*"><a title="([^"]+)" href="([^"]+)"', t):
        seg = t[m.end():m.end() + 800]
        om = re.search(r'field-price-old ">([\d.]+) ₫', seg)
        pm = re.search(r'field-price ?">(?:<span class="lbl-discount">[^<]*</span>)?(?:<span>)?([\d.]+)(?: |&nbsp;)₫', seg)
        vw = re.search(r'lbl-viewed">([\d.]+)', seg)
        r[m.group(2)] = dict(id=m.group(2).rsplit("-p", 1)[-1], name=H.unescape(m.group(1)), price=num(pm.group(1)) if pm else None, orig=num(om.group(1)) if om else None, views=vw.group(1) if vw else None)
    return list(r.values())
P.update(bonbanh=p_bonbanh, oto=p_oto, lm=p_lm)
if __name__ == "__main__": _main()

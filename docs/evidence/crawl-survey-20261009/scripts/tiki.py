import re, json, time, urllib.request, gzip
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
def get(u):
    t=time.time(); req=urllib.request.Request(u,headers={"User-Agent":UA,"Accept-Language":"vi-VN","Accept-Encoding":"gzip"})
    with urllib.request.urlopen(req,timeout=30) as r:
        b=r.read(); b=gzip.decompress(b) if r.headers.get("Content-Encoding")=="gzip" else b
        return r.status,b.decode("utf-8","replace"),time.time()-t
def walk(o,acc):
    if isinstance(o,dict):
        if "price" in o and ("name" in o) and ("id" in o or "product_id" in o): acc.append(o)
        for v in o.values(): walk(v,acc)
    elif isinstance(o,list):
        for v in o: walk(v,acc)
seen=set(); first=None
for page in (1,2,3):
    u="https://tiki.vn/dien-thoai-may-tinh-bang/c1789"+("" if page==1 else f"?page={page}")
    s,h,dt=get(u)
    m=re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',h,re.S)
    acc=[]; walk(json.loads(m.group(1)),acc) if m else None
    uniq={}; [uniq.setdefault(str(x.get("id")),x) for x in acc]
    new=[k for k in uniq if k not in seen]; seen.update(uniq)
    if first is None and uniq: first=list(uniq.values())[0]
    print(f"page {page}: HTTP {s} {len(h)//1024}KB {dt:.2f}s  products_in_NEXT_DATA={len(uniq)} new={len(new)}")
    time.sleep(2)
print("fields:", sorted(first.keys())[:45])
print("sample:", {k:first.get(k) for k in ("id","name","price","original_price","discount_rate","rating_average","review_count","quantity_sold","url_path","seller_name","brand_name") if k in first})
url="https://tiki.vn/"+first.get("url_path","").split("?")[0]
s,h,dt=get(url); ld=re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>',h,re.S)
print("product page:",url[:90],"HTTP",s,f"{dt:.2f}s","ld+json types:",[re.findall(r'"@type"\s*:\s*"(\w+)"',x)[:4] for x in ld], "next_data:", "__NEXT_DATA__" in h)
for x in ld:
    if '"Product"' in x:
        d=json.loads(x); o=d.get("offers",{}); print("  ld Product:",d.get("name","")[:50],"| sku",d.get("sku"),"| price",o.get("price") if isinstance(o,dict) else o,"| rating",d.get("aggregateRating",{}).get("ratingValue"))

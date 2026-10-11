import re, json, time, urllib.request, gzip, ssl, html as H
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
def get(u):
    req=urllib.request.Request(u,headers={"User-Agent":UA,"Accept-Language":"vi-VN","Accept-Encoding":"gzip"})
    try:
        t=time.time()
        with urllib.request.urlopen(req,timeout=40,context=ctx) as r:
            b=r.read(); b=gzip.decompress(b) if b[:2]==b"\x1f\x8b" else b; return r.status,b.decode("utf-8","replace"),time.time()-t
    except urllib.error.HTTPError as e: return e.code,"",0
    except Exception as e: return "ERR",str(e)[:80],0
def products(h):
    out=[]
    for x in re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>',h,re.S):
        try: d=json.loads(H.unescape(x.strip()))
        except Exception: 
            try: d=json.loads(x.strip())
            except Exception: continue
        st=d if isinstance(d,list) else d.get("@graph",[d]) if isinstance(d,dict) else []
        for o in st:
            if isinstance(o,dict) and o.get("@type")=="Product": out.append(o)
    return out
SITES={"tgdd":("https://www.thegioididong.com/dtdd",r'href="(/dtdd/[a-z0-9-]+)"',"https://www.thegioididong.com"),
 "dmx":("https://www.dienmayxanh.com/tivi",r"href='(/tivi/[a-z0-9-]+)","https://www.dienmayxanh.com"),
 "fptshop":("https://fptshop.com.vn/dien-thoai",r'href="(/dien-thoai/[a-z0-9-]+)"',"https://fptshop.com.vn"),
 "cellphones":("https://cellphones.com.vn/mobile.html",r'href="(https://cellphones\.com\.vn/[a-z0-9-]+\.html)"',""),
 "nguyenkim":("https://www.nguyenkim.com/dien-thoai.c",r'href="(https://www\.nguyenkim\.com/[a-z0-9-]+\.html)"',""),
 "phongvu":("https://phongvu.vn/c/laptop",r'href="(/[a-z0-9-]+--s\d+)"',"https://phongvu.vn"),
 "gearvn":("https://gearvn.com/collections/laptop",r'href="(/products/[a-z0-9-]+)"',"https://gearvn.com"),
 "dienmaycholon":("https://dienmaycholon.com/tivi",r'href="(https://dienmaycholon\.com/tivi/[a-z0-9-]+)"',""),
 "hoangha":("https://hoanghamobile.com/dien-thoai-di-dong",r'href="(/[a-z0-9-]{15,})"',"https://hoanghamobile.com"),
 "bachhoaxanh":("https://www.bachhoaxanh.com/sua-tuoi",r'href="(/sua-tuoi[a-z0-9-]*/[a-z0-9-]+)"',"https://www.bachhoaxanh.com"),
}
res=[]
for site,(lst,rx,base) in SITES.items():
    s,h,_=get(lst); links=[l for l in dict.fromkeys(re.findall(rx,h)) if not re.search(r"(-hang-|/hang-|sap-xep|filter|\?)",l)]
    got=None
    for l in links[3:9]:
        time.sleep(2); u=base+l if l.startswith("/") else l
        s2,h2,dt=get(u); ps=products(h2)
        if ps:
            o=ps[0]; off=o.get("offers",{}); off=off[0] if isinstance(off,list) and off else off
            got={"site":site,"url":u[:95],"http":s2,"secs":round(dt,2),"name":str(o.get("name"))[:55],"sku":o.get("sku") or o.get("mpn"),
                 "brand":(o.get("brand") or {}).get("name") if isinstance(o.get("brand"),dict) else o.get("brand"),
                 "offer_type":off.get("@type") if isinstance(off,dict) else None,"price":(off.get("price") or off.get("lowPrice")) if isinstance(off,dict) else None,
                 "currency":off.get("priceCurrency") if isinstance(off,dict) else None,"availability":str(off.get("availability",""))[-12:] if isinstance(off,dict) else None,
                 "rating":(o.get("aggregateRating") or {}).get("ratingValue")}
            break
    if not got: got={"site":site,"links_found":len(links),"result":"không thấy JSON-LD Product trên 6 trang chi tiết thử"}
    print(json.dumps(got,ensure_ascii=False)); res.append(got); time.sleep(2)
json.dump(res,open("detail.json","w",encoding="utf-8"),ensure_ascii=False,indent=1)

import re, json, time, urllib.request, gzip, ssl, html as H
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
def get(u):
    req=urllib.request.Request(u,headers={"User-Agent":UA,"Accept-Language":"vi-VN","Accept-Encoding":"gzip"})
    try:
        with urllib.request.urlopen(req,timeout=40,context=ctx) as r:
            b=r.read(); b=gzip.decompress(b) if b[:2]==b"\x1f\x8b" else b; return r.status,b.decode("utf-8","replace"),r.geturl()
    except urllib.error.HTTPError as e: return e.code,"",u
S=[ # site, page1, page2, product-url regex
 ("tgdd","https://www.thegioididong.com/dtdd","https://www.thegioididong.com/dtdd?pi=2",r'href="(/dtdd/[a-z0-9-]+)"'),
 ("dmx","https://www.dienmayxanh.com/tivi","https://www.dienmayxanh.com/tivi?pi=2",r"href='(/tivi/[a-z0-9-]+)"),
 ("fptshop","https://fptshop.com.vn/dien-thoai","https://fptshop.com.vn/dien-thoai?page=2",r'href="(/dien-thoai/[a-z0-9-]+)"'),
 ("cellphones","https://cellphones.com.vn/mobile.html","https://cellphones.com.vn/mobile.html?p=2",r'href="https://cellphones\.com\.vn/([a-z0-9-]+\.html)"'),
 ("nguyenkim","https://www.nguyenkim.com/dien-thoai.c","https://www.nguyenkim.com/dien-thoai.c?page=2",r'href="(?:https://www\.nguyenkim\.com)?(/[a-z0-9-]+\.html)"'),
 ("phongvu","https://phongvu.vn/c/laptop","https://phongvu.vn/c/laptop?page=2",r'href="(/[a-z0-9-]+--s\d+)"'),
 ("gearvn","https://gearvn.com/collections/laptop","https://gearvn.com/collections/laptop?page=2",r'href="(/products/[a-z0-9-]+)"'),
 ("dienmaycholon","https://dienmaycholon.com/tivi","https://dienmaycholon.com/tivi?page=2",r'href="(?:https://dienmaycholon\.com)?(/tivi/[a-z0-9-]+)"'),
 ("bachhoaxanh","https://www.bachhoaxanh.com/sua-tuoi","https://www.bachhoaxanh.com/sua-tuoi?page=2",r'href="(/sua-tuoi/[a-z0-9-]+)"'),
 ("hoangha","https://hoanghamobile.com/dien-thoai-di-dong","https://hoanghamobile.com/dien-thoai-di-dong?p=2",r'href="(/(?:dien-thoai-di-dong/)?[a-z0-9-]+-chinh-hang[a-z0-9-]*)"'),
]
out=[]
for site,p1,p2,rx in S:
    s1,h1,_=get(p1); time.sleep(2); s2,h2,_=get(p2)
    a=set(re.findall(rx,h1)); b=set(re.findall(rx,h2))
    ld=[]
    for x in re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>',h1,re.S):
        ld+=re.findall(r'"@type"\s*:\s*"(Product|ItemList|Offer|AggregateOffer)"',x)
    tot=re.findall(r"(\d[\d.]*)\s*(?:sản phẩm|kết quả)",H.unescape(re.sub(r"<[^>]+>"," ",h1)))
    r={"site":site,"p1":s1,"p1_products":len(a),"p2":s2,"p2_products":len(b),"p2_new":len(b-a),"ld_types":sorted(set(ld)),"total_hint":tot[:3]}
    print(json.dumps(r,ensure_ascii=False)); out.append(r); time.sleep(2)
json.dump(out,open("retail.json","w",encoding="utf-8"),ensure_ascii=False,indent=1)

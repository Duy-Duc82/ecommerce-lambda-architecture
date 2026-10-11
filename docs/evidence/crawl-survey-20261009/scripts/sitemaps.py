import re, json, time, urllib.request, gzip, ssl, html as H
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
def get(u):
    req=urllib.request.Request(u,headers={"User-Agent":UA,"Accept-Encoding":"gzip"})
    try:
        with urllib.request.urlopen(req,timeout=40,context=ctx) as r:
            b=r.read(); b=gzip.decompress(b) if b[:2]==b"\x1f\x8b" else b; return r.status,b.decode("utf-8","replace")
    except urllib.error.HTTPError as e: return e.code,""
    except Exception as e: return "ERR",""
for host in ["https://www.thegioididong.com","https://www.dienmayxanh.com","https://fptshop.com.vn","https://cellphones.com.vn","https://phongvu.vn","https://gearvn.com","https://dienmaycholon.com","https://hoanghamobile.com","https://www.lazada.vn","https://shopee.vn"]:
    s,h=get(host+"/robots.txt"); sm=re.findall(r"(?im)^\s*sitemap:\s*(\S+)",h)
    if not sm: sm=[host+"/sitemap.xml"]
    s2,h2=get(sm[0]); locs=re.findall(r"<loc>\s*(.*?)\s*</loc>",h2)
    print(json.dumps({"host":host,"robots_sitemaps":len(re.findall(r"(?im)^\s*sitemap:",h)),"first":sm[0][:80],"http":s2,"locs":len(locs),"sample":locs[:2]},ensure_ascii=False)); time.sleep(2)
# CPS + Hoang Ha real product detail
for u in ["https://cellphones.com.vn/dien-thoai-samsung-galaxy-a57.html"]:
    s,h=get(u); t=[re.findall(r'"@type"\s*:\s*"(\w+)"',x)[:3] for x in re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>',h,re.S)]
    pr=re.search(r'"price"\s*:\s*"?([\d.]+)',h); print("CPS detail",s,t,"price",pr.group(1) if pr else None)
s,h=get("https://hoanghamobile.com/dien-thoai-di-dong"); l=[x for x in dict.fromkeys(re.findall(r'href="(/[a-z0-9-]+-\d+gb[a-z0-9-]*)"',h))][:1]
if l:
    time.sleep(2); s,h=get("https://hoanghamobile.com"+l[0]); t=[re.findall(r'"@type"\s*:\s*"(\w+)"',x)[:3] for x in re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>',h,re.S)]
    pr=re.search(r'"price"\s*:\s*"?([\d.]+)',h); print("HoangHa detail",l[0],s,t,"price",pr.group(1) if pr else None)

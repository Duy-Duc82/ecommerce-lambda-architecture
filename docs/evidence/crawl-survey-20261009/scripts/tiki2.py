import re, json, time, urllib.request, gzip
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
def get(u):
    req=urllib.request.Request(u,headers={"User-Agent":UA,"Accept-Language":"vi-VN","Accept-Encoding":"gzip"})
    try:
        with urllib.request.urlopen(req,timeout=30) as r:
            b=r.read(); b=gzip.decompress(b) if (r.headers.get("Content-Encoding")=="gzip" or b[:2]==b"\x1f\x8b") else b
            return r.status,b.decode("utf-8","replace")
    except urllib.error.HTTPError as e: return e.code,""
# product page NEXT_DATA
s,h=get("https://tiki.vn/dien-thoai-samsung-galaxy-a27-5g-6gb-128gb-man-hinh-6-7-fhd-pin-5000mah-snapdragon-6-gen-3-hang-chinh-hang-p279551576.html")
m=re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',h,re.S); txt=m.group(1) if m else ""
for k in ("price","list_price","original_price","rating_average","review_count","inventory_status","current_seller"):
    mm=re.search(r'"%s"\s*:\s*("[^"]{0,40}"|[\d.]+|\{[^{}]{0,120})'%k,txt); print(" product NEXT_DATA",k,"=",mm.group(1)[:80] if mm else None)
# sitemaps
s,h=get("https://tiki.vn/robots.txt"); sm=re.findall(r"(?im)^sitemap:\s*(\S+)",h); print("robots sitemaps:",sm[:6])
for u in (sm[:2] or ["https://tiki.vn/sitemap.xml"]):
    s,h=get(u); locs=re.findall(r"<loc>(.*?)</loc>",h); print("sitemap",u,"HTTP",s,"locs",len(locs),locs[:3])
    time.sleep(2)

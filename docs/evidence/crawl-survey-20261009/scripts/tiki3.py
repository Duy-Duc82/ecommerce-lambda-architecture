import re, time, urllib.request, gzip
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
def get(u):
    req=urllib.request.Request(u,headers={"User-Agent":UA,"Accept-Encoding":"gzip"})
    with urllib.request.urlopen(req,timeout=60) as r:
        b=r.read(); b=gzip.decompress(b) if b[:2]==b"\x1f\x8b" else b; return b.decode("utf-8","replace")
idx=re.findall(r"<loc>(.*?)</loc>",get("https://tiki.vn/clover/sitemap_products_index.xml"))
print("product sitemap files:",len(idx),idx[:2])
time.sleep(2); locs=re.findall(r"<loc>(.*?)</loc>",get(idx[0])); print("urls in first file:",len(locs),locs[:2])
time.sleep(2); c=re.findall(r"<loc>(.*?)</loc>",get("https://tiki.vn/clover/sitemap_categories_index.xml")); print("category sitemap files:",len(c))

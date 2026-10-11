import re, json, time, sys, urllib.request, ssl, gzip, zlib
from urllib.parse import urlparse
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
CTX=ssl.create_default_context()
T=[
("websosanh","https://websosanh.vn/s/samsung+galaxy+a27"),
("chotot","https://www.chotot.com/mua-ban-dien-thoai"),
("hasaki","https://hasaki.vn/catalogsearch/result/?q=kem+chong+nang"),
("bachhoaxanh","https://www.bachhoaxanh.com/sua-tuoi"),
("phongvu","https://phongvu.vn/c/laptop"),
("gearvn","https://gearvn.com/collections/laptop"),
("nguyenkim","https://www.nguyenkim.com/dien-thoai-di-dong/"),
("longchau","https://nhathuoclongchau.com.vn/thuc-pham-chuc-nang"),
("pharmacity","https://www.pharmacity.vn/"),
("fahasa","https://www.fahasa.com/sach-trong-nuoc.html"),
("concung","https://concung.com/"),
("dienmaycholon","https://dienmaycholon.com/tivi"),
("didongviet","https://didongviet.vn/dien-thoai.html"),
("winmart","https://winmart.vn/"),
("sendo","https://www.sendo.vn/"),
("cellphones","https://cellphones.com.vn/mobile.html"),
("tgdd","https://www.thegioididong.com/dtdd"),
("dmx","https://www.dienmayxanh.com/tivi"),
("fptshop","https://fptshop.com.vn/dien-thoai"),
("tiki","https://tiki.vn/dien-thoai-may-tinh-bang/c1789"),
("guardian","https://www.guardian.com.vn/"),
("nhaccuatui_skip",None),
]
MARK={"ld_product":r'"@type"\s*:\s*"Product"',"ld_itemlist":r'"@type"\s*:\s*"ItemList"',"next_data":r"__NEXT_DATA__","next_rsc":r"self\.__next_f","nuxt":r"__NUXT","state":r"__INITIAL_STATE__|__PRELOADED_STATE__|__APOLLO_STATE__","json_price":r'\?"(?:price|salePrice|final_price|special_price)\?"\s*:\s*\?"?\d{4,}'}
BLOCK=r"captcha|verify/traffic|access denied|cf-chl|challenge-platform|Just a moment|baxia|cloudflare"
def get(url):
    req=urllib.request.Request(url,headers={"User-Agent":UA,"Accept":"text/html,*/*","Accept-Language":"vi-VN,vi;q=0.9","Accept-Encoding":"gzip, deflate"})
    try:
        with urllib.request.urlopen(req,timeout=25,context=CTX) as r:
            b=r.read();e=r.headers.get("Content-Encoding","")
            if e=="gzip":b=gzip.decompress(b)
            elif e=="deflate":b=zlib.decompress(b)
            return r.status,b.decode("utf-8","replace"),r.geturl()
    except urllib.error.HTTPError as e: return e.code,e.read().decode("utf-8","replace"),url
    except Exception as e: return "ERR:"+str(e)[:80],"",url
out=[]
for site,url in T:
    if not url: continue
    r={"site":site,"url":url}; t=time.time()
    st,h,fin=get(url); r.update(status=st,ms=int((time.time()-t)*1000),kb=len(h)//1024,final=fin)
    r["vnd"]=len(re.findall(r"\d{1,3}(?:[.,]\d{3})+\s*(?:₫|đ|&#x20AB;|&#8363;|VND|vnđ)",h,re.I))
    r["markers"]=[k for k,p in MARK.items() if re.search(p,h)]
    r["block"]=sorted(set(m.lower() for m in re.findall(BLOCK,h,re.I)))[:4]
    tt=re.search(r"<title[^>]*>(.*?)</title>",h,re.S|re.I); r["title"]=tt.group(1).strip()[:60] if tt else ""
    open(f"raw/{site}.html","w",encoding="utf-8").write(h)
    base=f"{urlparse(fin if fin.startswith('http') else url).scheme}://{urlparse(url).netloc}"
    rs,rb,_=get(base+"/robots.txt")
    r["robots_status"]=rs
    if rs==200:
        r["sitemaps"]=re.findall(r"(?im)^sitemap:\s*(\S+)",rb)[:3]
        # disallow rules for * agent (rough)
        r["disallow_n"]=len(re.findall(r"(?im)^disallow:\s*\S",rb))
        r["disallow_all"]=bool(re.search(r"(?im)^disallow:\s*/\s*$",rb))
        open(f"raw/{site}_robots.txt","w",encoding="utf-8").write(rb)
    out.append(r); print(json.dumps(r,ensure_ascii=False)); sys.stdout.flush(); time.sleep(1.5)
json.dump(out,open("probe.json","w",encoding="utf-8"),ensure_ascii=False,indent=1)

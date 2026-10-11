import re, json, time, sys, urllib.request, ssl, gzip, zlib
from targets import TARGETS
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
MARK={"ld_product":r'"@type"\s*:\s*"Product"',"ld_itemlist":r'"@type"\s*:\s*"ItemList"',"next_data":r"__NEXT_DATA__","next_rsc":r"self\.__next_f","nuxt":r"__NUXT","pageData":r"window\.pageData|__moduleData__","initial_state":r"__INITIAL_STATE__|__PRELOADED_STATE__|__APOLLO_STATE__","json_price":r'\?"(?:price|salePrice|final_price|priceShow)\?"\s*:\s*\?"?\d{4,}'}
BLOCK=r"captcha|punish|verify/traffic|access denied|cf-chl|challenge-platform|Just a moment|robot check|x5sec|baxia"
res=[]
for site,kind,url in TARGETS:
    t=time.time(); r={"site":site,"kind":kind,"url":url}
    try:
        req=urllib.request.Request(url,headers={"User-Agent":UA,"Accept":"text/html,application/xhtml+xml","Accept-Language":"vi-VN,vi;q=0.9","Accept-Encoding":"gzip, deflate"})
        with urllib.request.urlopen(req,timeout=30) as resp:
            b=resp.read(); enc=resp.headers.get("Content-Encoding","")
            if enc=="gzip": b=gzip.decompress(b)
            elif enc=="deflate": b=zlib.decompress(b)
            h=b.decode("utf-8","replace"); r.update(status=resp.status,final=resp.geturl(),server=resp.headers.get("Server"))
    except urllib.error.HTTPError as e:
        h=e.read().decode("utf-8","replace"); r.update(status=e.code,final=url,server=e.headers.get("Server"))
    except Exception as e:
        r.update(status="ERR",err=str(e)[:120]); res.append(r); print(r); continue
    r["ms"]=int((time.time()-t)*1000); r["kb"]=len(h)//1024
    r["vnd_text"]=len(re.findall(r"\d{1,3}(?:\.\d{3})+\s*(?:₫|đ|&#x20AB;|VND)",h))
    r["markers"]=[k for k,p in MARK.items() if re.search(p,h)]
    r["block"]=sorted(set(m.lower() for m in re.findall(BLOCK,h,re.I)))[:4]
    tt=re.search(r"<title[^>]*>(.*?)</title>",h,re.S|re.I); r["title"]=(tt.group(1).strip()[:70] if tt else "")
    open(f"raw/{site}_{kind}.html","w",encoding="utf-8").write(h)
    res.append(r); print(json.dumps(r,ensure_ascii=False)); sys.stdout.flush(); time.sleep(2)
json.dump(res,open("plain.json","w",encoding="utf-8"),ensure_ascii=False,indent=1)

import re, json, sys, glob, os
def ld(h):
    items=[]
    for m in re.finditer(r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>',h,re.S):
        try: j=json.loads(m.group(1).strip())
        except Exception: continue
        st=[j]
        while st:
            x=st.pop()
            if isinstance(x,list): st+=x; continue
            if not isinstance(x,dict): continue
            if x.get("@type")=="Product":
                o=x.get("offers") or {}
                if isinstance(o,list): o=o[0] if o else {}
                p=o.get("price") or o.get("lowPrice")
                items.append((x.get("sku") or x.get("productID") or x.get("url"),x.get("name"),p))
            st+=list(x.values())
    return items
def walk(h,key):
    m=re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',h,re.S)
    if not m: return []
    out=[];st=[json.loads(m.group(1))]
    while st:
        x=st.pop()
        if isinstance(x,list): st+=x
        elif isinstance(x,dict):
            nk=[k for k in ("name","title","subject","productName") if isinstance(x.get(k),str)]
            pk=[k for k in ("price","salePrice","sellPrice","latestPrice","finalPrice") if isinstance(x.get(k),(int,float)) and x.get(k)>1000]
            ik=[k for k in ("id","sku","list_id","ad_id","productId") if x.get(k) is not None]
            if nk and pk and ik: out.append((x[ik[0]],x[nk[0]][:50],x[pk[0]]))
            st+=list(x.values())
    return out
def rsc(h):
    # Next RSC: unescape then find objects with name & price
    s="".join(json.loads('"'+m+'"') for m in re.findall(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)',h))
    out=[]
    for m in re.finditer(r'"(?:name|displayName|productName)":"([^"]{5,90})"[^{}]{0,600}?"(?:price|sellPrice|currentPrice|salePrice|finalPrice)":(\d{4,})',s):
        out.append((None,m.group(1)[:50],int(m.group(2))))
    return out
for f in sorted(glob.glob("raw/*.html")):
    s=os.path.basename(f)[:-5]; h=open(f,encoding="utf-8").read()
    if len(h)<5000: continue
    a,b,c=ld(h),walk(h,None),rsc(h)
    best=max([("ld",a),("next",b),("rsc",c)],key=lambda t:len(set(map(str,t[1]))))
    u=list(dict.fromkeys(map(tuple,best[1])))
    print(f"{s:14} ld={len(a):3} next={len(b):3} rsc={len(c):3} -> {best[0]} uniq={len(u)}  e.g. {u[:2]}")

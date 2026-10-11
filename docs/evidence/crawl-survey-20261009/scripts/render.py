import re, json, time, sys
from playwright.sync_api import sync_playwright
T=[("lazada","listing","https://www.lazada.vn/dien-thoai-di-dong/"),
   ("lazada","search","https://www.lazada.vn/catalog/?q=iphone"),
   ("tiktokshop","listing","https://shop.tiktok.com/vn"),
   ("sendo","home","https://www.sendo.vn/"),
   ("hoangha","listing","https://hoanghamobile.com/dien-thoai-di-dong"),
   ("bachhoaxanh","listing","https://www.bachhoaxanh.com/sua-tuoi"),
   ("tiki","listing","https://tiki.vn/dien-thoai-may-tinh-bang/c1789")]
modes=sys.argv[1:] or ["headless"]
out=[]
with sync_playwright() as p:
  for mode in modes:
    b=p.chromium.launch(headless=(mode=="headless"))
    ctx=b.new_context(locale="vi-VN",ignore_https_errors=True,viewport={"width":1366,"height":900})
    for site,kind,url in T:
        if mode!="headless" and site not in ("lazada","tiktokshop"): continue
        pg=ctx.new_page(); xhr=[]
        pg.on("response",lambda r: xhr.append(r.status) if r.request.resource_type in("xhr","fetch") else None)
        r={"site":site,"kind":kind,"mode":mode}
        try:
            pg.goto(url,timeout=45000,wait_until="domcontentloaded"); pg.wait_for_timeout(9000)
            pg.mouse.wheel(0,3000); pg.wait_for_timeout(3000)
            txt=pg.inner_text("body")
            r.update(final=pg.url[:110],title=pg.title()[:60],prices=len(re.findall(r"\d{1,3}(?:\.\d{3})+\s*(?:₫|đ)",txt)),
                     sample=re.findall(r"\d{1,3}(?:\.\d{3})+\s*(?:₫|đ)",txt)[:3],xhr_total=len(xhr),xhr_blocked=sum(1 for s in xhr if s in(403,429)),
                     text=re.sub(r"\s+"," ",txt)[:160])
            pg.screenshot(path=f"raw/{site}_{kind}_{mode}.png")
        except Exception as e: r["err"]=str(e)[:150]
        print(json.dumps(r,ensure_ascii=False)); sys.stdout.flush(); out.append(r); pg.close(); time.sleep(3)
    b.close()
json.dump(out,open("render_"+"_".join(modes)+".json","w",encoding="utf-8"),ensure_ascii=False,indent=1)

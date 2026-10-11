import re,json,sys
from playwright.sync_api import sync_playwright
u=sys.argv[1]
with sync_playwright() as p:
    b=p.chromium.launch(headless=True); pg=b.new_page(locale="vi-VN")
    reqs=[]
    pg.on("response",lambda r: reqs.append((r.status,r.request.resource_type,r.url[:160])) if r.request.resource_type in("xhr","fetch","document") else None)
    pg.goto(u,wait_until="domcontentloaded",timeout=60000); pg.wait_for_timeout(6000)
    for _ in range(4): pg.mouse.wheel(0,3000); pg.wait_for_timeout(1000)
    t=pg.inner_text("body"); h=pg.content()
    open("sources/raw/wss_rendered.html","w",encoding="utf-8").write(h)
    print("title:",pg.title()); print("vnd in text:",len(re.findall(r"\d{1,3}(?:\.\d{3})+\s*đ",t)))
    for r in reqs: print(r)
    i=t.find("nơi bán"); print(t[i:i+2500])
    b.close()
